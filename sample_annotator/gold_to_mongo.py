import json
import logging
import math
import os
from datetime import datetime
from time import sleep
from typing import List, Optional, Set, Dict, Any
from urllib.parse import quote_plus
from tqdm import tqdm

import click
import dotenv
from diskcache import Cache
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from pymongo.uri_parser import parse_uri
from requests.auth import HTTPBasicAuth
import requests

import sample_annotator.clients.gold_client as gold_client

# Fix import path for both direct script execution and CLI entry point
try:
    # When running as a script
    from clients.gold_client import GoldClient, build_cache_key
except ModuleNotFoundError:
    # When running as an installed package
    from sample_annotator.clients.gold_client import GoldClient, build_cache_key

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_mongodb_credentials(env_file: Optional[str] = None) -> dict:
    """Load MongoDB credentials from environment file."""
    if env_file:
        dotenv.load_dotenv(env_file)
    else:
        default_env_path = os.path.join('local', '.env')
        if os.path.exists(default_env_path):
            dotenv.load_dotenv(default_env_path)
    return {
        'user': os.environ.get('MONGODB_USER'),
        'password': os.environ.get('MONGODB_PASSWORD')
    }


def build_mongodb_connection_string(
        mongo_uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None) -> str:
    """
    Injects user and password into an existing MongoDB URI if missing.
    If no URI is provided, falls back to localhost unauthenticated.
    """
    if mongo_uri:
        parsed = parse_uri(mongo_uri)
        if parsed.get('username') or parsed.get('password'):
            return mongo_uri  # already has credentials
        if not user or not password:
            return mongo_uri  # let it connect anonymously
        # Inject user/pass
        escaped_user = quote_plus(user)
        escaped_password = quote_plus(password)
        protocol_split = mongo_uri.split("://", 1)
        if len(protocol_split) != 2:
            raise ValueError("Invalid MongoDB URI")
        scheme, rest = protocol_split
        return f"{scheme}://{escaped_user}:{escaped_password}@{rest}"

    if not user or not password:
        return "mongodb://localhost:27017/"
    escaped_user = quote_plus(user)
    escaped_password = quote_plus(password)
    return f"mongodb://{escaped_user}:{escaped_password}@localhost:27017/"


def create_unique_index(collection, field_name: str, index_name: str) -> None:
    """Create a unique index on a MongoDB collection."""
    try:
        collection.create_index([(field_name, ASCENDING)], name=index_name, unique=True)
    except Exception as e:
        logging.error(f"Failed to create index '{index_name}': {e}")


def insert_document(collection, document: dict, key_name: str) -> bool:
    """Insert a document into MongoDB, handling duplicate key errors."""
    try:
        collection.insert_one(document)
        return True
    except DuplicateKeyError:
        logging.warning(f"Duplicate key error for {key_name}")
        return False


def process_study_ids(file_path: str) -> List[str]:
    """Extract study IDs from a file."""
    ids = set()
    with open(file_path) as file:
        for line in file:
            line = line.strip()
            if line.startswith("Gs"):
                ids.add(line)
    if not ids:
        raise ValueError(f"No valid IDs found in {file_path}")
    return sorted(ids)


def get_processed_study_ids(db) -> Set[str]:
    """Get a set of study IDs that have already been processed in MongoDB."""
    study_collection = db['studies']
    processed_ids = set()
    for study in study_collection.find({}, {'studyGoldId': 1}):
        if 'studyGoldId' in study:
            processed_ids.add(study['studyGoldId'])
    return processed_ids


class EnhancedGoldClient(GoldClient):
    """Enhanced version of GoldClient with direct cache access."""

    def _call(self, endpoint: str, params: Dict = {}) -> Any:
        """
        Enhanced version of _call that directly checks the cache before
        using the memoized function.
        """
        (user, passwd) = self.gold_key
        endpoint_url = f"{self.url}/{endpoint}"

        # Check both cache key formats
        key_data = build_cache_key(endpoint_url, params, user, passwd)
        simple_key = f"{endpoint_url}-{params.get('studyGoldId', '')}-{user}-{passwd}"

        # Try to get directly from cache first
        if gold_client.cache and key_data in gold_client.cache:
            logging.info(f"[cache] Direct cache hit for {endpoint} {params}")
            return gold_client.cache.get(key_data)
        elif gold_client.cache and simple_key in gold_client.cache:
            logging.info(f"[cache] Simple key cache hit for {endpoint} {params}")
            return gold_client.cache.get(simple_key)

        # Fall back to original method if not in cache
        logging.warning(f"[cache] Cache miss for {endpoint} {params}, making API call")
        self.num_calls += 1

        # Make API call directly rather than using memoize
        attempt = 0
        while attempt < 4:
            try:
                results = requests.get(
                    endpoint_url, params=params, auth=HTTPBasicAuth(user, passwd)
                )
                logging.info(f"STATUS={results.status_code}")
                if results.status_code == 200:
                    data = results.json()
                    # Store in cache for future use
                    if gold_client.cache:
                        gold_client.cache.set(key_data, data)
                        gold_client.cache.set(simple_key, data)
                    return data
                else:
                    logging.error(
                        f"API call to {endpoint_url} failed, code={results.status_code}; attempt={attempt} [pausing]"
                    )
                    sleep(5 ** attempt)
            except Exception as e:
                logging.error(f"Exception during API call: {e}")
                sleep(5 ** attempt)

            attempt += 1

        raise Exception(f"API call to {endpoint_url} failed after {attempt} attempts")


@click.command()
@click.option('--authentication-file', '-a', default="config/gold-key.txt",
              help="Path to GOLD API authentication file (user:pass)")
@click.option('--env-file', '-e', help="Path to environment file with MongoDB credentials")
@click.option('--log-failures-to-file', type=click.Path(writable=True), default=None,
              help="Path to write failure logs as JSON")
@click.option('--max-retries', '-m', type=int, default=3,
              help="Maximum number of retries for failed API calls")
@click.option('--mongo-uri', '-u', required=True,
              help="MongoDB URI including database name")
@click.option('--purge-diskcache', '-P', is_flag=True, default=False,
              help="Purge the disk cache before processing")
@click.option('--purge-mongodb', '-p', is_flag=True, default=False,
              help="Purge the MongoDB collections before processing")
@click.option('--resume', '-r', is_flag=True, default=True,
              help="Resume from last run, skipping already processed studies")
@click.option('--study-ids-file', '-i', type=click.Path(exists=True), required=True,
              help="File containing study IDs to process")
@click.option('--cache-dir', default="cachedir",
              help="Path to diskcache directory")
@click.option('--batch-size', type=int, default=50,
              help="Number of studies to process in each batch")
@click.option('--progress-bar/--no-progress-bar', default=True,
              help="Show progress bars for processing")
def main(study_ids_file: str, authentication_file: str, mongo_uri: Optional[str],
         env_file: Optional[str], purge_mongodb: bool, purge_diskcache: bool,
         resume: bool, max_retries: int, log_failures_to_file: Optional[str],
         cache_dir: str, batch_size: int, progress_bar: bool) -> None:
    """
    Load GOLD metadata into MongoDB using a local cache to minimize API calls.

    This script processes study IDs from a file, fetches their metadata from GOLD API
    or a local cache, and stores the data in MongoDB collections.
    """
    # Connect to MongoDB
    mongo_creds = load_mongodb_credentials(env_file)
    conn_str = build_mongodb_connection_string(
        mongo_uri=mongo_uri,
        user=mongo_creds.get('user'),
        password=mongo_creds.get('password'),
    )

    logging.info("Connecting to MongoDB")
    client = MongoClient(conn_str)

    try:
        db_name = MongoClient(conn_str).get_default_database().name
        db = client[db_name]
    except Exception as e:
        logging.error(f"Could not determine database name from URI: {e}")
        return

    try:
        client.admin.command('ping')
        logging.info("MongoDB connection successful")
    except Exception as e:
        logging.error(f"MongoDB connection failed: {e}")
        return

    # Set up MongoDB collections
    if purge_mongodb:
        logging.info("Purging MongoDB collections...")
        db.drop_collection('biosamples')
        db.drop_collection('studies')
        db.drop_collection('projects')
        db.drop_collection('seq_projects')
        db.drop_collection('study_import_failures')
        resume = False

    biosample_collection = db['biosamples']
    study_collection = db['studies']
    project_collection = db['seq_projects']
    failure_collection = db['study_import_failures']

    # Create indices for faster lookups and uniqueness constraints
    create_unique_index(biosample_collection, "biosampleGoldId", "biosampleGoldId_index")
    create_unique_index(study_collection, "studyGoldId", "studyGoldId_index")
    create_unique_index(project_collection, "projectGoldId", "projectGoldId_index")
    create_unique_index(failure_collection, "studyGoldId", "failedStudy_index")

    # Initialize the cache for GoldClient
    gold_client.set_cache_directory(cache_dir)
    logging.info(f"Using disk cache directory: {cache_dir}")

    # Create enhanced GoldClient that checks cache directly
    gc = EnhancedGoldClient()

    if purge_diskcache:
        logging.info("Purging disk cache...")
        gc.clear_cache()

    gc.load_key(authentication_file)

    # Process study IDs from file
    study_ids = process_study_ids(study_ids_file)
    total_studies = len(study_ids)
    logging.info(f"Found {total_studies} studies to process")

    # Identify studies that have already been processed
    processed_study_ids = set()
    if resume:
        processed_study_ids = get_processed_study_ids(db)
        if processed_study_ids:
            already_processed = len(processed_study_ids)
            logging.info(f"Found {already_processed} studies already in MongoDB that will be skipped")
            logging.info(f"Remaining studies to process: {total_studies - already_processed}")

    # Process studies in batches
    completed = 0
    failed = 0
    skipped = 0
    failed_study_logs = []

    # Determine number of batches
    num_batches = math.ceil(total_studies / batch_size)

    # Process each batch
    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, total_studies)
        batch = study_ids[batch_start:batch_end]

        logging.info(f"Processing batch {batch_num + 1}/{num_batches} ({len(batch)} studies)")

        # Use tqdm for progress bar if enabled
        iterator = tqdm(batch, desc=f"Batch {batch_num + 1}") if progress_bar else batch

        # Process each study in the batch
        for study_id in iterator:
            if resume and study_id in processed_study_ids:
                logging.info(f"Skipping study {study_id} (already in MongoDB)")
                skipped += 1
                continue

            logging.info(f"Processing study {study_id} ({completed + skipped + 1}/{total_studies})...")

            retry_count = 0
            success = False

            while retry_count <= max_retries and not success:
                try:
                    # Fetch study data
                    study = gc.fetch_study(study_id)
                    if not study:
                        logging.warning(f"No data returned for study {study_id}, skipping")
                        failed += 1
                        failure_doc = {
                            'studyGoldId': study_id,
                            'error': "No data returned (null response)",
                            'timestamp': datetime.utcnow(),
                            'failed': True
                        }
                        insert_document(failure_collection, failure_doc, study_id)
                        failed_study_logs.append(failure_doc)
                        break

                    # Fetch and process biosamples
                    biosamples = gc.fetch_biosamples_by_study(study_id)
                    logging.info(f"Retrieved {len(biosamples)} biosamples for study {study_id}")

                    # Store biosamples and their projects
                    biosample_ids = []
                    for biosample in biosamples:
                        biosample_id = biosample.get('biosampleGoldId')
                        if biosample_id:
                            biosample_ids.append(biosample_id)

                        # Extract projects before inserting biosample
                        projects = biosample.pop('projects', [])

                        # Insert biosample
                        insert_document(biosample_collection, biosample, biosample_id)

                        # Insert projects
                        for project in projects:
                            insert_document(project_collection, project, project.get('projectGoldId', 'Unknown'))

                    # Add biosample IDs to study and insert into MongoDB
                    study['biosamples'] = biosample_ids
                    insert_document(study_collection, study, study_id)

                    completed += 1
                    success = True

                except Exception as e:
                    retry_count += 1
                    if retry_count <= max_retries:
                        wait_time = 5 * retry_count
                        logging.warning(
                            f"Error processing study {study_id}: {e}. Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})")
                        sleep(wait_time)
                    else:
                        logging.error(f"Failed to process study {study_id} after {max_retries} attempts: {e}")
                        failed += 1
                        failure_doc = {
                            'studyGoldId': study_id,
                            'error': str(e),
                            'timestamp': datetime.utcnow(),
                            'failed': True
                        }
                        insert_document(failure_collection, failure_doc, study_id)
                        failed_study_logs.append(failure_doc)

    # Report results
    logging.info(f"Import completed: {completed} studies processed, {skipped} skipped, {failed} failed")

    # Write failure logs if requested
    if log_failures_to_file and failed_study_logs:
        with open(log_failures_to_file, 'w') as f:
            json.dump(failed_study_logs, f, indent=2, default=str)
        logging.info(f"Wrote {len(failed_study_logs)} failed study logs to {log_failures_to_file}")

    # Close connections
    client.close()
    logging.info("MongoDB connection closed")


if __name__ == "__main__":
    main()
