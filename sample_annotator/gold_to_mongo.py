import json
import logging
import os
from datetime import datetime
from time import sleep
from typing import List, Optional, Set
from urllib.parse import quote_plus

import click
import dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from pymongo.uri_parser import parse_uri

# Fix import path for both direct script execution and CLI entry point
try:
    # When running as a script
    from clients.gold_client import GoldClient
except ModuleNotFoundError:
    # When running as an installed package
    from sample_annotator.clients.gold_client import GoldClient

# todo might need better API error handling
#   should be more consistent about bundling (projects in biosamples) vs getting biosamples separate from studies

# todo document the fact that a biosamples key is added to studies
#   biosamples have no foreign keys
#   (sequencing) projects include native study and biosample foreign keys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_mongodb_credentials(env_file: Optional[str] = None) -> dict:
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
        # Find where to insert credentials
        protocol_split = mongo_uri.split("://", 1)
        if len(protocol_split) != 2:
            raise ValueError("Invalid MongoDB URI")
        scheme, rest = protocol_split
        return f"{scheme}://{escaped_user}:{escaped_password}@{rest}"

    # If no URI provided, build a basic local one
    if not user or not password:
        return "mongodb://localhost:27017/"
    escaped_user = quote_plus(user)
    escaped_password = quote_plus(password)
    return f"mongodb://{escaped_user}:{escaped_password}@localhost:27017/"


def create_unique_index(collection, field_name: str, index_name: str) -> None:
    try:
        collection.create_index([(field_name, ASCENDING)], name=index_name, unique=True)
    except Exception as e:
        logging.error(f"Failed to create index '{index_name}': {e}")


def insert_document(collection, document: dict, key_name: str) -> None:
    try:
        collection.insert_one(document)
    except DuplicateKeyError:
        logging.warning(f"Duplicate key error for {key_name}")


def process_study_ids(file_path: str) -> List[str]:
    ids = set()
    with open(file_path) as file:
        for line in file:
            if line.startswith("Gs"):
                ids.add(line.strip())
    if not ids:
        raise ValueError(f"No valid IDs found in {file_path}")
    return sorted(ids)


def get_processed_study_ids(db) -> Set[str]:
    study_collection = db['studies']
    processed_ids = set()
    for study in study_collection.find({}, {'studyGoldId': 1}):
        if 'studyGoldId' in study:
            processed_ids.add(study['studyGoldId'])
    return processed_ids


@click.command()
@click.option('--authentication-file', '-a', default="config/gold-key.txt")
@click.option('--env-file', '-e')
@click.option('--log-failures-to-file', type=click.Path(writable=True), default=None)
@click.option('--max-retries', '-m', type=int, default=3)
@click.option('--mongo-uri', '-u', required=True)
@click.option('--purge-diskcache', '-P', is_flag=True, default=False)
@click.option('--purge-mongodb', '-p', is_flag=True, default=False)
@click.option('--resume', '-r', is_flag=True, default=True)
@click.option('--study-ids-file', '-i', type=click.Path(exists=True), required=True)
def main(study_ids_file: str, authentication_file: str, mongo_uri: Optional[str],
         env_file: Optional[str], purge_mongodb: bool, purge_diskcache: bool,
         resume: bool, max_retries: int, log_failures_to_file: Optional[str]):
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

    create_unique_index(biosample_collection, "biosampleGoldId", "biosampleGoldId_index")
    create_unique_index(study_collection, "studyGoldId", "studyGoldId_index")
    create_unique_index(project_collection, "projectGoldId", "projectGoldId_index")
    create_unique_index(failure_collection, "studyGoldId", "failedStudy_index")

    gc = GoldClient()
    if purge_diskcache:
        logging.info("Purging disk cache...")
        gc.clear_cache()
    gc.load_key(authentication_file)

    study_ids = process_study_ids(study_ids_file)
    total_studies = len(study_ids)

    processed_study_ids = set()
    if resume:
        processed_study_ids = get_processed_study_ids(db)
        if processed_study_ids:
            logging.info(f"Found {len(processed_study_ids)} studies already in MongoDB that will be skipped")

    completed = 0
    failed = 0
    skipped = 0
    failed_study_logs = []

    for study_id in study_ids:
        if resume and study_id in processed_study_ids:
            logging.info(f"Skipping study {study_id} (already in MongoDB)")
            skipped += 1
            continue

        logging.info(f"Processing study {study_id} ({completed + skipped + 1}/{total_studies})...")

        retry_count = 0
        success = False

        while retry_count <= max_retries and not success:
            try:
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

                biosamples = gc.fetch_biosamples_by_study(study_id)
                logging.info(f"Retrieved {len(biosamples)} biosamples for study {study_id}")

                biosample_ids = []
                for biosample in biosamples:
                    biosample_id = biosample.get('biosampleGoldId')
                    if biosample_id:
                        biosample_ids.append(biosample_id)

                    for project in biosample.pop('projects', []):
                        insert_document(project_collection, project, project.get('projectGoldId', 'Unknown'))
                    insert_document(biosample_collection, biosample, biosample_id)

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

    logging.info(f"Import completed: {completed} studies processed, {skipped} skipped, {failed} failed")

    if log_failures_to_file and failed_study_logs:
        with open(log_failures_to_file, 'w') as f:
            json.dump(failed_study_logs, f, indent=2, default=str)
        logging.info(f"Wrote failed study logs to {log_failures_to_file}")

    client.close()
    logging.info("MongoDB connection closed")


if __name__ == "__main__":
    main()
