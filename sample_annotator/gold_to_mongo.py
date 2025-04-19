import logging
import os
from time import sleep
from typing import List, Optional, Set
from urllib.parse import quote_plus

import click
import dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

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
#   biosamples kave no foreign keys
#   (sequencing) projects include native study and biosample foreign keys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_mongodb_credentials(env_file: Optional[str] = None) -> dict:
    """
    Loads MongoDB credentials from environment variables or .env file.
    
    Args:
        env_file: Optional path to .env file. If not provided, will try to load from local/.env

    Returns:
        Dictionary with MongoDB connection credentials
    """
    # Try to load from .env file if specified or from default location
    if env_file:
        dotenv.load_dotenv(env_file)
    else:
        default_env_path = os.path.join('local', '.env')
        if os.path.exists(default_env_path):
            dotenv.load_dotenv(default_env_path)

    # Get credentials from environment variables
    creds = {
        'user': os.environ.get('MONGODB_USER'),
        'password': os.environ.get('MONGODB_PASSWORD')
    }

    return creds


def build_mongodb_connection_string(
        mongo_uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: str = 'localhost',
        port: str = '27017',
        auth_source: str = 'admin',
        auth_mechanism: str = 'SCRAM-SHA-256'
) -> str:
    """
    Builds a MongoDB connection string based on provided parameters.
    
    Args:
        mongo_uri: Optional complete MongoDB URI (overrides other parameters if provided)
        user: Username for authentication
        password: Password for authentication
        host: MongoDB host
        port: MongoDB port
        auth_source: Authentication database
        auth_mechanism: Authentication mechanism

    Returns:
        MongoDB connection string
    """
    # If URI is provided, use it directly
    if mongo_uri:
        return mongo_uri

    # If no authentication is required
    if not user or not password:
        return f"mongodb://{host}:{port}/"

    # Build authenticated connection string
    escaped_user = quote_plus(user)
    escaped_password = quote_plus(password)
    conn_str = (
        f"mongodb://{escaped_user}:{escaped_password}@{host}:{port}/"
        f"?authSource={auth_source}&authMechanism={auth_mechanism}"
    )

    return conn_str


def create_unique_index(collection, field_name: str, index_name: str) -> None:
    """
    Creates a unique index on the specified field for a MongoDB collection.

    Args:
        collection: The MongoDB collection object.
        field_name: The field to index.
        index_name: The name of the index.
    """
    try:
        collection.create_index([(field_name, ASCENDING)], name=index_name, unique=True)
    except Exception as e:
        logging.error(f"Failed to create index '{index_name}': {e}")


def insert_document(collection, document: dict, key_name: str) -> None:
    """
    Inserts a document into a MongoDB collection, handling duplicate key errors.

    Args:
        collection: The MongoDB collection object.
        document: The document to insert.
        key_name: The key used for the unique index, for logging purposes.
    """
    try:
        collection.insert_one(document)
    except DuplicateKeyError as e:
        # logging.warning(f"Duplicate key error for {key_name}: {e}")
        logging.warning(f"Duplicate key error for {key_name}")


def process_study_ids(file_path: str) -> List[str]:
    """
    Reads and processes study IDs from a file.

    Args:
        file_path: Path to the file containing study IDs.

    Returns:
        A sorted list of unique study IDs.
    """
    ids = set()
    with open(file_path) as file:
        for line in file:
            if line.startswith("Gs"):
                ids.add(line.strip())
    if not ids:
        raise ValueError(f"No valid IDs found in {file_path}")
    return sorted(ids)


def get_processed_study_ids(db) -> Set[str]:
    """
    Gets the IDs of studies that have already been processed and stored in MongoDB.
    
    Args:
        db: The MongoDB database object
        
    Returns:
        A set of study IDs that have already been processed
    """
    study_collection = db['studies']
    processed_ids = set()
    
    # Find all studies that have already been stored
    for study in study_collection.find({}, {'studyGoldId': 1}):
        if 'studyGoldId' in study:
            processed_ids.add(study['studyGoldId'])
            
    return processed_ids


@click.command()
@click.option('--study-ids-file', '-i',
              type=click.Path(exists=True, dir_okay=False, readable=True),
              required=True,
              help='Path to the input text file containing one GOLD study ID per line.')
@click.option('--authentication-file', '-a', default="config/gold-key.txt",
              help='Path to the GOLD authentication file. Contents should be user:pass.')
@click.option('--mongo-uri', '-u', required=True,
              help='MongoDB connection URI. If provided, this overrides other MongoDB connection options.')
@click.option('--env-file', '-e',
              help='Path to .env file with MongoDB credentials. Default: local/.env')
@click.option('--purge-mongodb', '-p', is_flag=True, default=False,
              help='Purge the destination MongoDB collections before running.')
@click.option('--purge-diskcache', '-P', is_flag=True, default=False,
              help='Purge the input disk cache before running.')
@click.option('--resume', '-r', is_flag=True, default=True,
              help='Skip studies that are already in MongoDB. Default: True')              
@click.option('--max-retries', '-m', type=int, default=3,
              help='Maximum number of retries for failed GOLD API calls. Default: 3')
def main(study_ids_file: str, authentication_file: str,
         mongo_uri: Optional[str] = None, env_file: Optional[str] = None,
         purge_mongodb: bool = False, purge_diskcache: bool = False, 
         resume: bool = True, max_retries: int = 3, **args):
    """
    Fetch, process, and store biosamples, studies, and projects into MongoDB in real-time.
    
    Supports both local and remote MongoDB servers with authentication.
    
    Environment variables (from .env file or system):
        MONGODB_USER: MongoDB username
        MONGODB_PASSWORD: MongoDB password

    If needed, must be passed in the URI
        MONGODB_HOST: MongoDB host (default: localhost)
        MONGODB_PORT: MongoDB port (default: 27017)
        MONGODB_AUTH_SOURCE: Authentication database (default: admin)
        MONGODB_AUTH_MECHANISM: Authentication mechanism (default: SCRAM-SHA-256)
    """
    # Load MongoDB credentials
    mongo_creds = load_mongodb_credentials(env_file)

    # Build connection string and connect to MongoDB
    if not mongo_uri:
        logging.error("Missing required --mongo-uri option. Database name must be embedded in the URI.")
        return

    conn_str = build_mongodb_connection_string(
        mongo_uri=mongo_uri,
        user=mongo_creds.get('user'),
        password=mongo_creds.get('password'),
    )

    logging.info(f"Connecting to MongoDB at {mongo_creds.get('host')}:{mongo_creds.get('port')}")
    client = MongoClient(conn_str)

    try:
        db_name = MongoClient(conn_str).get_default_database().name
        db = client[db_name]
    except Exception as e:
        logging.error(f"Could not determine database name from URI: {e}")
        return

    # Test connection
    try:
        # Ping the server to check connection
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
        # Reset the resume flag since we're starting fresh
        resume = False

    # Setup collections and indexes
    biosample_collection = db['biosamples']
    study_collection = db['studies']
    project_collection = db['seq_projects']

    create_unique_index(biosample_collection, "biosampleGoldId", "biosampleGoldId_index")
    create_unique_index(study_collection, "studyGoldId", "studyGoldId_index")
    create_unique_index(project_collection, "projectGoldId", "projectGoldId_index")

    # Initialize GoldClient
    gc = GoldClient()

    if purge_diskcache:
        logging.info("Purging disk cache...")
        gc.clear_cache()

    gc.load_key(authentication_file)

    # Process study IDs
    study_ids = process_study_ids(study_ids_file)
    total_studies = len(study_ids)
    
    # Get already processed studies if in resume mode
    processed_study_ids = set()
    if resume:
        processed_study_ids = get_processed_study_ids(db)
        if processed_study_ids:
            logging.info(f"Found {len(processed_study_ids)} studies already in MongoDB that will be skipped")
    
    # Track progress
    completed = 0
    failed = 0
    skipped = 0

    for study_id in study_ids:
        # Skip if already processed and in resume mode
        if resume and study_id in processed_study_ids:
            logging.info(f"Skipping study {study_id} (already in MongoDB)")
            skipped += 1
            continue
            
        logging.info(f"Processing study {study_id} ({completed + skipped + 1}/{total_studies})...")

        # Retry logic for API failures
        retry_count = 0
        success = False
        
        while retry_count <= max_retries and not success:
            try:
                # Fetch the study record
                study = gc.fetch_study(study_id, **args)
                
                if not study:
                    logging.warning(f"No data returned for study {study_id}, skipping")
                    failed += 1
                    break

                # Fetch biosamples associated with the study
                biosamples = gc.fetch_biosamples_by_study(study_id, **args)
                logging.info(f"Retrieved {len(biosamples)} biosamples for study {study_id}")

                # Process was successful
                success = True
                
                # Collect biosampleGoldIds for the study
                biosample_ids = []

                for biosample in biosamples:
                    biosample_id = biosample.get('biosampleGoldId', None)
                    if biosample_id:
                        biosample_ids.append(biosample_id)

                    # Handle associated projects
                    for project in biosample.pop('projects', []):
                        insert_document(project_collection, project, project.get('projectGoldId', 'Unknown'))

                    # Insert biosample into MongoDB
                    insert_document(biosample_collection, biosample, biosample_id)

                # Add the biosamples list to the study record
                study['biosamples'] = biosample_ids

                # Insert the study record into MongoDB
                insert_document(study_collection, study, study_id)
                
                completed += 1
                
            except Exception as e:
                retry_count += 1
                if retry_count <= max_retries:
                    wait_time = 5 * retry_count
                    logging.warning(f"Error processing study {study_id}: {e}. Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})")
                    sleep(wait_time)
                else:
                    logging.error(f"Failed to process study {study_id} after {max_retries} attempts: {e}")
                    failed += 1

    # Log summary statistics
    logging.info(f"Import completed: {completed} studies processed, {skipped} skipped, {failed} failed")

    # Close the connection
    client.close()
    logging.info("MongoDB connection closed")


if __name__ == "__main__":
    main()
