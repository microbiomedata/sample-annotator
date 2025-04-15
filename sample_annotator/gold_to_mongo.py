import logging
import os
from typing import List, Optional, Set
from urllib.parse import quote_plus

import click
import dotenv
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

from clients.gold_client import GoldClient

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
        'password': os.environ.get('MONGODB_PASSWORD'),
        'host': os.environ.get('MONGODB_HOST', 'localhost'),
        'port': os.environ.get('MONGODB_PORT', '27017'),
        'auth_source': os.environ.get('MONGODB_AUTH_SOURCE', 'admin'),
        'auth_mechanism': os.environ.get('MONGODB_AUTH_MECHANISM', 'SCRAM-SHA-256')
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


@click.command()
@click.option('--mongo-db-name', '-d', required=True,
              help='Name of the MongoDB database to use.')
@click.option('--study-ids-file', '-i',
              type=click.Path(exists=True, dir_okay=False, readable=True),
              required=True,
              help='Path to the input text file containing one GOLD study ID per line.')
@click.option('--authentication-file', '-a', default="config/gold-key.txt",
              help='Path to the GOLD authentication file. Contents should be user:pass.')
@click.option('--mongo-uri', '-u',
              help='MongoDB connection URI. If provided, this overrides other MongoDB connection options.')
@click.option('--env-file', '-e',
              help='Path to .env file with MongoDB credentials. Default: local/.env')
@click.option('--purge-mongodb', '-p', is_flag=True, default=False,
              help='Purge the destination MongoDB collections before running.')
@click.option('--purge-diskcache', '-P', is_flag=True, default=False,
              help='Purge the input disk cache before running.')
def main(mongo_db_name: str, study_ids_file: str, authentication_file: str,
         mongo_uri: Optional[str] = None, env_file: Optional[str] = None,
         purge_mongodb: bool = False, purge_diskcache: bool = False, **args):
    """
    Fetch, process, and store biosamples, studies, and projects into MongoDB in real-time.
    
    Supports both local and remote MongoDB servers with authentication.
    
    Environment variables (from .env file or system):
        MONGODB_USER: MongoDB username
        MONGODB_PASSWORD: MongoDB password
        MONGODB_HOST: MongoDB host (default: localhost)
        MONGODB_PORT: MongoDB port (default: 27017)
        MONGODB_AUTH_SOURCE: Authentication database (default: admin)
        MONGODB_AUTH_MECHANISM: Authentication mechanism (default: SCRAM-SHA-256)
    """
    # Load MongoDB credentials
    mongo_creds = load_mongodb_credentials(env_file)
    
    # Build connection string and connect to MongoDB
    conn_str = build_mongodb_connection_string(
        mongo_uri=mongo_uri,
        user=mongo_creds.get('user'),
        password=mongo_creds.get('password'),
        host=mongo_creds.get('host'),
        port=mongo_creds.get('port'),
        auth_source=mongo_creds.get('auth_source'),
        auth_mechanism=mongo_creds.get('auth_mechanism')
    )
    
    logging.info(f"Connecting to MongoDB at {mongo_creds.get('host')}:{mongo_creds.get('port')}")
    client = MongoClient(conn_str)
    db = client[mongo_db_name]
    
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

    # Setup collections and indexes
    biosample_collection = db['biosamples']
    study_collection = db['studies']
    project_collection = db['projects']

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

    for study_id in study_ids:
        logging.info(f"Processing study {study_id}...")

        # Fetch the study record
        study = gc.fetch_study(study_id, **args)

        # Fetch biosamples associated with the study
        biosamples = gc.fetch_biosamples_by_study(study_id, **args)
        logging.info(f"Retrieved {len(biosamples)} biosamples for study {study_id}")

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

    # Close the connection
    client.close()
    logging.info("MongoDB connection closed")


if __name__ == "__main__":
    main()
