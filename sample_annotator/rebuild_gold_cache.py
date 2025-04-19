import hashlib
import logging
import os
import pickle
from tqdm import tqdm

import click
import dotenv
from pymongo import MongoClient
from diskcache import Cache

from sample_annotator.clients.gold_client import build_cache_key

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_BASE = "https://gold.jgi.doe.gov/rest/nmdc"


def load_mongodb_credentials(env_file: str) -> dict:
    if env_file:
        dotenv.load_dotenv(env_file)
    return {
        'user': os.environ.get('MONGODB_USER'),
        'password': os.environ.get('MONGODB_PASSWORD')
    }


def load_gold_key(path: str) -> tuple[str, str]:
    with open(path) as f:
        return tuple(f.read().strip().split(":"))


def normalize_id(id_str: str) -> str:
    return id_str.replace("gold:", "")


def calculate_key_hash(key_data):
    """Calculate the hash of a key for logging purposes."""
    key_bytes = pickle.dumps(key_data)
    hashed_key = hashlib.sha256(key_bytes).hexdigest()
    return hashed_key


@click.command()
@click.option('--mongo-uri', required=True, help='MongoDB URI including database name')
@click.option('--env-file', '-e', required=False, help='Path to .env file with MongoDB user/pass')
@click.option('--authentication-file', required=True, help='GOLD API key file (user:pass)')
@click.option('--cache-dir', default="cachedir", help='Path to diskcache directory (default: cachedir)')
@click.option('--debug', is_flag=True, help='Print extra debug information')
@click.option('--batch-size', default=1000, help='Batch size for processing biosamples')
def rebuild_gold_cache(mongo_uri: str, env_file: str, authentication_file: str, cache_dir: str,
                       debug: bool, batch_size: int):
    """
    Rebuild diskcache entries for GoldClient by inserting MongoDB-stored responses
    directly into the cache without calling the API again.
    """
    mongo_user = mongo_password = None
    if env_file:
        mongo_creds = load_mongodb_credentials(env_file)
        mongo_user = mongo_creds.get('user')
        mongo_password = mongo_creds.get('password')

    # Connect to MongoDB
    logging.info(f"Connecting to MongoDB: {mongo_uri}")
    client = MongoClient(mongo_uri, username=mongo_user, password=mongo_password)
    db = client.get_default_database()

    # Load GOLD API credentials
    user, password = load_gold_key(authentication_file)

    # Initialize diskcache
    cache = Cache(cache_dir)

    # Clear the cache first to avoid any issues with existing entries
    logging.info(f"Clearing existing cache at {cache_dir}")
    cache.clear()

    # Get counts for progress tracking
    studies_count = db.studies.count_documents({})
    projects_count = db.seq_projects.count_documents({})
    biosamples_count = db.biosamples.count_documents({})

    logging.info(
        f"Found {studies_count} studies, {projects_count} projects, and {biosamples_count} biosamples in MongoDB")

    # --------------------------
    # Phase 1: Cache studies and projects
    # --------------------------
    logging.info("Phase 1: Caching studies and projects")

    # Create lookup of biosample IDs to study IDs
    logging.info("Building biosample to study mapping from projects...")
    biosample_to_study = {}
    for project in tqdm(db.seq_projects.find({}, {'studyGoldId': 1, 'biosampleGoldId': 1}),
                        total=projects_count, desc="Building biosample mapping"):
        bs_id = project.get('biosampleGoldId')
        study_id = project.get('studyGoldId')
        if bs_id and study_id:
            biosample_to_study[bs_id] = study_id

    logging.info(f"Created mapping for {len(biosample_to_study)} biosamples to their studies")

    # Find all study IDs
    study_ids = [doc['studyGoldId'] for doc in db.studies.find({}, {'studyGoldId': 1, '_id': 0})]

    # Process each study
    study_count = 0
    project_count = 0

    for study_id in tqdm(study_ids, desc="Caching studies and projects"):
        norm_study_id = normalize_id(study_id)

        # Get the full study document
        study = db.studies.find_one({'studyGoldId': study_id}, {'_id': 0})
        if not study:
            continue

        # Cache study data using both formats
        study_key = build_cache_key(f"{API_BASE}/studies", {"studyGoldId": norm_study_id}, user, password)
        cache.set(study_key, [study])

        # Also cache with a simpler key format for the memoize decorator
        simple_study_key = f"{API_BASE}/studies-{norm_study_id}-{user}-{password}"
        cache.set(simple_study_key, [study])

        study_count += 1

        # Get and cache projects for this study
        projects = list(db.seq_projects.find({'studyGoldId': study_id}, {'_id': 0}))
        if projects:
            projects_key = build_cache_key(f"{API_BASE}/projects", {"studyGoldId": norm_study_id}, user, password)
            cache.set(projects_key, projects)

            # Also cache with simpler key format
            simple_projects_key = f"{API_BASE}/projects-{norm_study_id}-{user}-{password}"
            cache.set(simple_projects_key, projects)

            project_count += len(projects)

            if debug:
                logging.info(f"Cached {len(projects)} projects for study {study_id}")

    logging.info(f"Cached {study_count} studies and {project_count} projects")

    # --------------------------
    # Phase 2: Cache biosamples by study
    # --------------------------
    logging.info("Phase 2: Caching biosamples by study")

    # Create a dictionary to group biosamples by study
    biosamples_by_study = {}

    # Process biosamples in batches to avoid memory issues
    biosample_count = 0
    total_batches = (biosamples_count + batch_size - 1) // batch_size

    for batch_num in tqdm(range(total_batches), desc="Processing biosample batches"):
        skip = batch_num * batch_size

        batch = list(db.biosamples.find({}, {'_id': 0}).skip(skip).limit(batch_size))

        for biosample in batch:
            bs_id = biosample.get('biosampleGoldId')
            if not bs_id or bs_id not in biosample_to_study:
                continue

            study_id = biosample_to_study[bs_id]
            norm_study_id = normalize_id(study_id)

            if norm_study_id not in biosamples_by_study:
                biosamples_by_study[norm_study_id] = []

            biosamples_by_study[norm_study_id].append(biosample)
            biosample_count += 1

    # Cache the biosamples by study
    logging.info(f"Caching {len(biosamples_by_study)} study-biosample groups")

    for study_id, biosamples in tqdm(biosamples_by_study.items(), desc="Caching biosamples by study"):
        # Cache using both key formats
        bs_key = build_cache_key(f"{API_BASE}/biosamples", {"studyGoldId": study_id}, user, password)
        cache.set(bs_key, biosamples)

        simple_bs_key = f"{API_BASE}/biosamples-{study_id}-{user}-{password}"
        cache.set(simple_bs_key, biosamples)

        if debug:
            logging.info(f"Cached {len(biosamples)} biosamples for study {study_id}")

    # Report on cache size and contents
    logging.info(f"Cache now contains {len(cache)} records")
    logging.info(f"  - {study_count} studies")
    logging.info(f"  - {project_count} projects")
    logging.info(f"  - {biosample_count} biosamples")

    cache.close()
    logging.info(f"Cache closed and flushed to disk at {cache_dir}")


if __name__ == '__main__':
    rebuild_gold_cache()
