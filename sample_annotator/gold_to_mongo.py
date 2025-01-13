import logging
from typing import List, Set

import click
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
              help='Name of the local, unauthenticated MongoDB database to use.')
@click.option('--study-ids-file', '-i',
              type=click.Path(exists=True, dir_okay=False, readable=True),
              required=True,
              help='Path to the input text file containing one GOLD study ID per line.')
@click.option('--authentication-file', '-a', default="config/gold-key.txt",
              help='Path to the authentication file. Contents should be user:pass.')
@click.option('--purge-mongodb', '-p', is_flag=True, default=False,
              help='Purge the destination MongoDB database before running.')
@click.option('--purge-diskcache', '-P', is_flag=True, default=False,
              help='Purge the input disk cache before running.')
def main(mongo_db_name: str, study_ids_file: str, authentication_file: str,
         purge_mongodb: bool, purge_diskcache: bool, **args):
    """
    Fetch, process, and store biosamples, studies, and projects into MongoDB in real-time.
    """
    # MongoDB setup
    client = MongoClient('mongodb://localhost:27017/')
    db = client[mongo_db_name]

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


if __name__ == "__main__":
    main()
