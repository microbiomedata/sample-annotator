import logging
import os

import click
import dotenv
from pymongo import MongoClient
from diskcache import Cache

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


def normalize_id(id_str: str) -> str:
    return id_str.replace("gold:", "")


@click.command()
@click.option('--mongo-uri', required=True, help='MongoDB URI including database name')
@click.option('--env-file', '-e', required=False, help='Path to .env file with MongoDB user/pass')
@click.option('--cache-dir', default="cachedir", help='Path to diskcache directory (default: cachedir)')
def rebuild_gold_cache(mongo_uri: str, env_file: str, cache_dir: str):
    """
    Rebuild diskcache entries for GoldClient by inserting MongoDB-stored responses
    directly into the cache without calling the API again.
    """
    mongo_creds = load_mongodb_credentials(env_file) if env_file else {'user': None, 'password': None}
    user = mongo_creds.get('user')
    password = mongo_creds.get('password')

    # If no credentials are present, continue unauthenticated
    if not user or not password:
        logging.warning("No GOLD credentials found in .env; proceeding without authentication")
        user = ""
        password = ""

    logging.info(f"Connecting to MongoDB: {mongo_uri}")
    client = MongoClient(mongo_uri, username=mongo_creds.get('user'), password=mongo_creds.get('password'))
    db = client.get_default_database()

    cache = Cache(cache_dir)

    studies = list(db.studies.find({}, {'_id': 0}))
    biosamples = list(db.biosamples.find({}, {'_id': 0}))
    projects = list(db.seq_projects.find({}, {'_id': 0}))

    # DEBUG: Confirm MongoDB contents
    logging.info(f"Sample study: {studies[0] if studies else 'EMPTY'}")
    logging.info(f"Sample biosample: {biosamples[0] if biosamples else 'EMPTY'}")
    logging.info(f"Sample project: {projects[0] if projects else 'EMPTY'}")

    logging.info(
        f"Inserting {len(studies)} studies, {len(biosamples)} biosamples, and {len(projects)} projects into cache")

    for i, study in enumerate(studies):
        study_id = normalize_id(study["studyGoldId"])
        key = (f"{API_BASE}/studies", ("studyGoldId", study_id), user, password)
        cache.set(key, [study])

    biosamples_by_study = {}
    for prj in projects:
        sid = prj.get("studyGoldId")
        bsid = prj.get("biosampleGoldId")
        if sid and bsid:
            sid_norm = normalize_id(sid)
            biosamples_by_study.setdefault(sid_norm, []).append(bsid)

    biosample_lookup = {b["biosampleGoldId"]: b for b in biosamples if "biosampleGoldId" in b}

    for sid, bsids in biosamples_by_study.items():
        bs_payload = [biosample_lookup[bid] for bid in bsids if bid in biosample_lookup]
        if not bs_payload:
            continue
        key = (f"{API_BASE}/biosamples", ("studyGoldId", sid), user, password)
        cache.set(key, bs_payload)

    cache.close()
    logging.info(f"Cache closed and flushed to disk at {cache_dir}")


if __name__ == '__main__':
    rebuild_gold_cache()
