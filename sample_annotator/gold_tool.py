import hashlib
import json
import logging
import os
import pickle
import sys
from datetime import datetime
from time import sleep
from typing import Dict, List, Optional, Set, Any, Union, TextIO, Tuple
from urllib.parse import quote_plus
from tqdm import tqdm

import click
import dotenv
import requests
import yaml
from diskcache import Cache
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from pymongo.uri_parser import parse_uri
from requests.auth import HTTPBasicAuth

# Type definitions
USERPASS = Tuple[str, str]
URL = str
JSON = Any
SampleDict = JSON
StudyDict = JSON
ProjectDict = JSON
ApDict = JSON
FILENAME = Union[str, bytes, os.PathLike]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global cache
cache = None
API_BASE = "https://gold.jgi.doe.gov/rest/nmdc"

# Exclusion list for problematic study IDs
EXCLUSION_LIST = []


def ensure_cache_initialized(cache_dir="cachedir"):
    """Ensure the cache is initialized."""
    global cache
    if cache is None:
        cache = Cache(cache_dir)
        logging.info(f"Cache initialized at {cache_dir} with {len(cache)} records")


def build_cache_key(endpoint_url: str, params: dict, user: str, passwd: str) -> tuple:
    """Build a cache key in a consistent format."""
    normalized_params = tuple(sorted(params.items()))
    key_data = (endpoint_url, normalized_params, user, passwd)
    return key_data


def normalize_id(id_str: str) -> str:
    """Normalize GOLD IDs by removing 'gold:' prefix."""
    return id_str.replace("gold:", "")


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
    """Build MongoDB connection string with credentials."""
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


def fetch_url(endpoint_url: str, params: Dict, user: str, passwd: str, max_retries: int = 3) -> JSON:
    """Fetch data from GOLD API with retries."""
    logging.info(f"API call to {endpoint_url} with params {params}")
    attempt = 0
    while attempt <= max_retries:
        try:
            results = requests.get(
                endpoint_url, params=params, auth=HTTPBasicAuth(user, passwd)
            )
            logging.info(f"STATUS={results.status_code}")
            if results.status_code == 200:
                return results.json()
            else:
                logging.error(
                    f"API call failed, code={results.status_code}; attempt={attempt} [pausing]"
                )
        except Exception as e:
            logging.error(f"API call exception: {e}; attempt={attempt} [pausing]")

        # Retry with backoff
        attempt += 1
        if attempt <= max_retries:
            wait_time = 5 * attempt
            logging.info(f"Retrying in {wait_time} seconds...")
            sleep(wait_time)

    raise Exception(f"API call to {endpoint_url} failed after {max_retries + 1} attempts")


class GoldClient:
    """
    A client for the GOLD API with caching support.
    Can fetch studies, biosamples, projects, and other data.
    """

    def __init__(self, cache_dir: str = "cachedir"):
        self.gold_key = None
        self.url = API_BASE
        self.num_calls = 0
        ensure_cache_initialized(cache_dir)

    def load_key(self, path: str) -> None:
        """Load API key from file."""
        with open(path) as stream:
            lines = stream.readlines()
            [user, passwd] = lines[0].strip().split(":")
            self.gold_key = user, passwd

    def clear_cache(self) -> None:
        """Clear the cache."""
        global cache
        if cache:
            cache.clear()
            logging.info("Cache cleared")

    def _normalize_id(self, id: str) -> str:
        """Normalize GOLD IDs."""
        return normalize_id(id)

    def _call(self, endpoint: str, params: Dict = {}) -> JSON:
        """
        Call GOLD API with caching.
        First checks cache, then falls back to API call.
        """
        (user, passwd) = self.gold_key
        endpoint_url = f"{self.url}/{endpoint}"

        # Try cache first
        key = build_cache_key(endpoint_url, params, user, passwd)
        if key in cache:
            logging.info(f"Cache hit for {endpoint} {params}")
            return cache.get(key)

        # Fall back to API call
        logging.info(f"Cache miss for {endpoint} {params}")
        data = fetch_url(endpoint_url, params, user, passwd)

        # Cache the result
        cache.set(key, data)
        self.num_calls += 1

        return data

    def fetch_projects_by_study(self, id: str) -> List[SampleDict]:
        """Fetch projects for a study."""
        id = self._normalize_id(id)
        results = self._call("projects", {"studyGoldId": id})
        return results

    def fetch_biosamples_by_study(self, id: str, include_project=True) -> List[SampleDict]:
        """
        Fetch biosamples for a study.
        Optionally includes project data.
        """
        id = self._normalize_id(id)
        if id in EXCLUSION_LIST:
            biosamples = []
        else:
            biosamples = self._call("biosamples", {"studyGoldId": id})
            if include_project:
                projects = self.fetch_projects_by_study(id)

                # Map biosamples by ID for easy lookup
                samples_by_id = {
                    sample["biosampleGoldId"]: sample for sample in biosamples
                }

                # Add projects to their biosamples
                for project in projects:
                    sample_id = project.get("biosampleGoldId")
                    if not sample_id or sample_id not in samples_by_id:
                        continue

                    sample = samples_by_id[sample_id]
                    if "projects" not in sample:
                        sample["projects"] = []
                    sample["projects"].append(project)

        return biosamples

    def fetch_study(self, id: str, include_biosamples=False) -> StudyDict:
        """
        Fetch a study by ID.
        Optionally includes biosamples.
        """
        id = self._normalize_id(id)
        logging.info(f"Fetching study: {id}")
        results = self._call("studies", {"studyGoldId": id})

        if not results:
            logging.warning(f"No study found for ID: {id}")
            return {}

        study = results[0]
        if include_biosamples:
            study["biosamples"] = self.fetch_biosamples_by_study(id)

        return study

    def fetch_studies(self, ids: List[str], **kwargs) -> List[StudyDict]:
        """Fetch multiple studies."""
        logging.info(f"Fetching {len(ids)} studies")
        studies = []
        for id in tqdm(ids, desc="Fetching studies"):
            studies.append(self.fetch_study(id, **kwargs))
        return studies

    def rebuild_cache_from_mongodb(
            self,
            mongo_uri: str,
            mongo_user: Optional[str] = None,
            mongo_password: Optional[str] = None
    ) -> None:
        """
        Rebuild the cache from MongoDB data.
        Caches studies, biosamples, and projects.
        """
        logging.info(f"Rebuilding cache from MongoDB: {mongo_uri}")

        # Connect to MongoDB
        conn_str = build_mongodb_connection_string(mongo_uri, mongo_user, mongo_password)
        client = MongoClient(conn_str)
        db = client.get_default_database()

        # Load collections
        studies = list(db.studies.find({}, {'_id': 0}))
        biosamples = list(db.biosamples.find({}, {'_id': 0}))
        projects = list(db.seq_projects.find({}, {'_id': 0}))

        logging.info(f"Found {len(studies)} studies, {len(biosamples)} biosamples, "
                     f"and {len(projects)} projects in MongoDB")

        # Clear existing cache
        self.clear_cache()

        # Cache studies
        for study in tqdm(studies, desc="Caching studies"):
            study_id = normalize_id(study["studyGoldId"])
            key = build_cache_key(f"{self.url}/studies", {"studyGoldId": study_id},
                                  self.gold_key[0], self.gold_key[1])
            cache.set(key, [study])  # API returns as list

        # Group biosamples by study
        biosamples_by_study = {}
        projects_by_study = {}

        # Group projects by study and build biosample->study mapping
        biosample_to_study = {}
        for project in tqdm(projects, desc="Processing projects"):
            study_id = project.get("studyGoldId")
            biosample_id = project.get("biosampleGoldId")

            if study_id:
                study_id = normalize_id(study_id)
                if study_id not in projects_by_study:
                    projects_by_study[study_id] = []
                projects_by_study[study_id].append(project)

            if biosample_id and study_id:
                biosample_to_study[biosample_id] = normalize_id(study_id)

        # Group biosamples by study
        for biosample in tqdm(biosamples, desc="Processing biosamples"):
            biosample_id = biosample.get("biosampleGoldId")
            if biosample_id in biosample_to_study:
                study_id = biosample_to_study[biosample_id]
                if study_id not in biosamples_by_study:
                    biosamples_by_study[study_id] = []
                biosamples_by_study[study_id].append(biosample)

        # Cache projects
        for study_id, study_projects in tqdm(projects_by_study.items(), desc="Caching projects"):
            key = build_cache_key(f"{self.url}/projects", {"studyGoldId": study_id},
                                  self.gold_key[0], self.gold_key[1])
            cache.set(key, study_projects)

        # Cache biosamples
        for study_id, study_biosamples in tqdm(biosamples_by_study.items(), desc="Caching biosamples"):
            key = build_cache_key(f"{self.url}/biosamples", {"studyGoldId": study_id},
                                  self.gold_key[0], self.gold_key[1])
            cache.set(key, study_biosamples)

        logging.info(f"Cache rebuild complete. Cache now contains {len(cache)} entries.")

    def load_to_mongodb(
            self,
            study_ids: List[str],
            mongo_uri: str,
            mongo_user: Optional[str] = None,
            mongo_password: Optional[str] = None,
            resume: bool = True,
            max_retries: int = 3,
            batch_size: int = 50
    ) -> Dict:
        """
        Load GOLD data for specified studies into MongoDB.
        Uses cache when available, falls back to API calls.
        Returns statistics about the operation.
        """
        logging.info(f"Loading {len(study_ids)} studies to MongoDB: {mongo_uri}")

        # Connect to MongoDB
        conn_str = build_mongodb_connection_string(mongo_uri, mongo_user, mongo_password)
        client = MongoClient(conn_str)
        db = client.get_default_database()

        # Set up collections
        biosample_collection = db['biosamples']
        study_collection = db['studies']
        project_collection = db['seq_projects']
        failure_collection = db['study_import_failures']

        # Create indices
        create_unique_index(biosample_collection, "biosampleGoldId", "biosampleGoldId_index")
        create_unique_index(study_collection, "studyGoldId", "studyGoldId_index")
        create_unique_index(project_collection, "projectGoldId", "projectGoldId_index")
        create_unique_index(failure_collection, "studyGoldId", "failedStudy_index")

        # Find already processed studies
        processed_ids = set()
        if resume:
            processed_ids = get_processed_study_ids(db)
            if processed_ids:
                logging.info(f"Found {len(processed_ids)} studies already in MongoDB that will be skipped")

        # Process studies in batches
        stats = {"completed": 0, "skipped": 0, "failed": 0}
        failed_studies = []

        # Calculate number of batches
        num_batches = (len(study_ids) + batch_size - 1) // batch_size

        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(study_ids))
            batch = study_ids[start_idx:end_idx]

            logging.info(f"Processing batch {batch_idx + 1}/{num_batches} ({len(batch)} studies)")

            for study_id in tqdm(batch, desc=f"Batch {batch_idx + 1}"):
                if resume and study_id in processed_ids:
                    logging.info(f"Skipping study {study_id} (already in MongoDB)")
                    stats["skipped"] += 1
                    continue

                logging.info(f"Processing study {study_id} "
                             f"({stats['completed'] + stats['skipped'] + 1}/{len(study_ids)})...")

                retry_count = 0
                success = False

                while retry_count <= max_retries and not success:
                    try:
                        # Fetch study and biosamples
                        study = self.fetch_study(study_id)
                        if not study:
                            logging.warning(f"No data returned for study {study_id}, skipping")
                            stats["failed"] += 1
                            failed_studies.append({
                                'studyGoldId': study_id,
                                'error': "No data returned (null response)",
                                'timestamp': datetime.utcnow().isoformat(),
                                'failed': True
                            })
                            break

                        biosamples = self.fetch_biosamples_by_study(study_id)
                        logging.info(f"Retrieved {len(biosamples)} biosamples for study {study_id}")

                        # Process biosamples and projects
                        biosample_ids = []
                        for biosample in biosamples:
                            biosample_id = biosample.get('biosampleGoldId')
                            if biosample_id:
                                biosample_ids.append(biosample_id)

                            # Extract and insert projects
                            for project in biosample.pop('projects', []):
                                insert_document(project_collection, project,
                                                project.get('projectGoldId', 'Unknown'))

                            # Insert biosample
                            insert_document(biosample_collection, biosample, biosample_id)

                        # Insert study with biosample references
                        study['biosamples'] = biosample_ids
                        insert_document(study_collection, study, study_id)

                        stats["completed"] += 1
                        success = True

                    except Exception as e:
                        retry_count += 1
                        if retry_count <= max_retries:
                            wait_time = 5 * retry_count
                            logging.warning(
                                f"Error processing study {study_id}: {e}. "
                                f"Retrying in {wait_time} seconds "
                                f"(attempt {retry_count}/{max_retries})")
                            sleep(wait_time)
                        else:
                            logging.error(f"Failed to process study {study_id} after "
                                          f"{max_retries} attempts: {e}")
                            stats["failed"] += 1
                            failed_studies.append({
                                'studyGoldId': study_id,
                                'error': str(e),
                                'timestamp': datetime.utcnow().isoformat(),
                                'failed': True
                            })
                            insert_document(failure_collection, {
                                'studyGoldId': study_id,
                                'error': str(e),
                                'timestamp': datetime.utcnow(),
                                'failed': True
                            }, study_id)

        # Close connection
        client.close()
        logging.info(f"Import completed: {stats['completed']} studies processed, "
                     f"{stats['skipped']} skipped, {stats['failed']} failed")

        return {
            "stats": stats,
            "failed_studies": failed_studies
        }


@click.group()
@click.option('-v', '--verbose', count=True, help='Increase verbosity')
@click.option('-q', '--quiet', is_flag=True, help='Decrease verbosity')
@click.option('--cache-dir', default="cachedir", help='Path to cache directory')
@click.pass_context
def cli(ctx, verbose, quiet, cache_dir):
    """GOLD API client with caching and MongoDB integration."""
    # Set up logging
    if verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif verbose == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)
    if quiet:
        logging.basicConfig(level=logging.ERROR)

    # Initialize context
    ctx.ensure_object(dict)
    ctx.obj['cache_dir'] = cache_dir
    ensure_cache_initialized(cache_dir)


@cli.command()
@click.argument('idfile')
@click.option('-o', '--output', type=click.File('w'), default=sys.stdout, help='Output file')
@click.option('-O', '--output-format', type=click.Choice(['json', 'yaml']), default='json',
              help='Output format')
@click.option('--include-biosamples/--no-include-biosamples', default=False,
              help='Include biosamples in study data')
@click.option('--clear-cache/--no-clear-cache', default=False, help='Clear cache before fetching')
@click.option('-a', '--authentication-file', default="config/gold-key.txt",
              help='Path to authentication file')
@click.pass_context
def fetch_studies(ctx, idfile, output, output_format, include_biosamples, clear_cache,
                  authentication_file):
    """
    Fetch studies from GOLD API and save as JSON or YAML.

    IDFILE is a file containing GOLD study IDs (Gs...) one per line.
    """
    # Initialize client
    gc = GoldClient(ctx.obj['cache_dir'])
    gc.load_key(authentication_file)

    if clear_cache:
        gc.clear_cache()

    # Read study IDs
    ids = []
    with open(idfile) as file:
        for line in file:
            if line.startswith("Gs"):
                ids.append(line.strip())

    if not ids:
        raise click.BadParameter(f"No study IDs found in {idfile}")

    # Fetch studies
    studies = gc.fetch_studies(ids, include_biosamples=include_biosamples)

    # Write output
    if output_format == 'yaml':
        yaml.dump(studies, output, default_flow_style=False, sort_keys=False)
    else:
        json.dump(studies, output, indent=2)

    logging.info(f"Fetched {len(studies)} studies. API calls: {gc.num_calls}")


@cli.command()
@click.option('-i', '--study-ids-file', required=True, type=click.Path(exists=True),
              help='File containing study IDs')
@click.option('-u', '--mongo-uri', required=True, help='MongoDB URI')
@click.option('-e', '--env-file', help='Environment file with MongoDB credentials')
@click.option('-a', '--authentication-file', default="config/gold-key.txt",
              help='GOLD API authentication file')
@click.option('--log-failures-to-file', type=click.Path(), help='Write failures to JSON file')
@click.option('-r', '--resume/--no-resume', default=True, help='Skip already processed studies')
@click.option('-b', '--batch-size', default=50, help='Number of studies per batch')
@click.option('-m', '--max-retries', default=3, help='Maximum retry attempts')
@click.pass_context
def load_to_mongodb(ctx, study_ids_file, mongo_uri, env_file, authentication_file,
                    log_failures_to_file, resume, batch_size, max_retries):
    """
    Load GOLD data into MongoDB.

    Fetches studies, biosamples, and projects from GOLD API
    and stores them in MongoDB collections.
    """
    # Load MongoDB credentials if provided
    mongo_creds = load_mongodb_credentials(env_file) if env_file else {}

    # Initialize client
    gc = GoldClient(ctx.obj['cache_dir'])
    gc.load_key(authentication_file)

    # Read study IDs
    study_ids = process_study_ids(study_ids_file)

    # Load to MongoDB
    result = gc.load_to_mongodb(
        study_ids=study_ids,
        mongo_uri=mongo_uri,
        mongo_user=mongo_creds.get('user'),
        mongo_password=mongo_creds.get('password'),
        resume=resume,
        max_retries=max_retries,
        batch_size=batch_size
    )

    # Write failures to file if requested
    if log_failures_to_file and result['failed_studies']:
        with open(log_failures_to_file, 'w') as f:
            json.dump(result['failed_studies'], f, indent=2)
        logging.info(f"Wrote {len(result['failed_studies'])} failed study logs to {log_failures_to_file}")


@cli.command()
@click.option('--mongo-uri', required=True, help='MongoDB URI including database name')
@click.option('-e', '--env-file', help='Path to .env file with MongoDB credentials')
@click.option('-a', '--authentication-file', default="config/gold-key.txt",
              help='GOLD API authentication file')
@click.pass_context
def rebuild_cache(ctx, mongo_uri, env_file, authentication_file):
    """
    Rebuild cache from MongoDB data.

    Uses existing MongoDB collections to populate cache
    without making API calls.
    """
    # Load MongoDB credentials if provided
    mongo_creds = load_mongodb_credentials(env_file) if env_file else {}

    # Initialize client
    gc = GoldClient(ctx.obj['cache_dir'])
    gc.load_key(authentication_file)

    # Rebuild cache
    gc.rebuild_cache_from_mongodb(
        mongo_uri=mongo_uri,
        mongo_user=mongo_creds.get('user'),
        mongo_password=mongo_creds.get('password')
    )


@cli.command()
@click.pass_context
def inspect_cache(ctx):
    """
    Print information about cache contents.
    """
    c = Cache(ctx.obj['cache_dir'])
    click.echo(f"Cache directory: {c.directory}")
    click.echo(f"Cache contains {len(c)} records")

    # Print some example keys
    for i, key in enumerate(c):
        if i >= 10:  # limit to 10 entries
            break
        value = c.get(key)
        click.echo(f"Key {i}: {key}")
        if isinstance(value, list):
            click.echo(f"  Value type: list with {len(value)} items")
            if value and 'studyGoldId' in value[0]:
                click.echo(f"  Study ID: {value[0]['studyGoldId']}")
        else:
            click.echo(f"  Value type: {type(value)}")


if __name__ == "__main__":
    cli(obj={})
