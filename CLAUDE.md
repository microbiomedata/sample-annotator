# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation Files

- `ABOUT.md`: General project information (last updated February 2024)
- `CLAUDE.md`: Instructions for Claude Code (this file, updated April 2025)
- `CONTRIBUTING.md`: Guidelines for contributors (last updated February 2024)
- `gold-knowledge-management.md`: Technical documentation for GOLD metadata integration (updated April 2025)
- `README.md`: Main project overview and getting started guide (last updated February 2024)
- `sphinx/static/intro.md`: Introduction for generated documentation (last updated February 2024)

## Build and Test Commands

- Install dependencies: `poetry install`
- Run all tests: `poetry run pytest` or `make test`
- Run single test file: `poetry run pytest -sv tests/test_<file>.py`
- Run specific test: `poetry run pytest -sv tests/test_<file>.py::TestClass::test_method`
- Run tools: `poetry run annotate-sample [args]` (see other scripts in pyproject.toml)
- Lint/formatting: `poetry run black .` and `poetry run autopep8 .`
- Build package: `poetry build` (creates wheel in dist/ directory)

## Code Style Guidelines

- Python 3.10+ with type hints in function signatures
- Follow PEP-8 conventions with Black formatting
- Classes use CamelCase, functions/methods use snake_case
- Use NumPy-style docstrings
- Imports: standard library first, third-party second, local modules last
- Error handling via structured AnnotationReport class with severity levels
- Test files should match module structure and use pytest assertions

## Data Integration Workflows

### GOLD Biosamples → MongoDB Pipeline
The repository contains a complete pipeline for fetching data from the GOLD API and storing it in MongoDB:

- Located in `sample_annotator/gold_to_mongo.py` and `make-gold-cache.Makefile`
- Pipeline fetches studies, biosamples, and projects from GOLD and creates proper relationships
- Data model structure:
  - `biosamples` collection with unique index on `biosampleGoldId`
  - `studies` collection with unique index on `studyGoldId`, including array of associated biosample IDs
  - `projects` collection with unique index on `projectGoldId`
- A complete gold_metadata database should have the following collections:
  - `biosamples`: Core sample metadata from GOLD (created by this pipeline)
  - `studies`: Study information with links to biosamples (created by this pipeline)
  - `projects`: Project data with links to studies and biosamples (created by this pipeline)
  - `flattened_biosample_contacts`: Contact information extracted from biosamples (created by other codebases)
  - `flattened_biosamples`: Denormalized biosample data (created by other codebases)
  - `notes`: Additional metadata notes (created by other codebases)
- Usage: `make -f make-gold-cache.Makefile load-gold-biosamples-into-mongo`
- Configuration options:
  - MongoDB database name
  - GOLD authentication file
  - Study IDs file for selective import
  - Options to purge existing data (MongoDB and disk cache)
  - Remote MongoDB connection with authentication:
    - Via environment: Create `local/.env` file based on `local/.env.example`
    - Via command line: Use `--mongo-uri` and/or `--env-file` options
- Prerequisites:
  - MongoDB server (local or remote)
  - For authenticated connections, credentials in .env file or as URI
  - GOLD API credentials in config/gold-key.txt
  - List of GOLD study IDs to process
- Environment variables (in .env file):
  - `MONGODB_USER`: MongoDB username
  - `MONGODB_PASSWORD`: MongoDB password
  - `MONGODB_HOST`: MongoDB host (default: localhost)
  - `MONGODB_PORT`: MongoDB port (default: 27017)
  - `MONGODB_AUTH_SOURCE`: Authentication database (default: admin)
  - `MONGODB_AUTH_MECHANISM`: Authentication mechanism (default: SCRAM-SHA-256)

### GOLD Data Processing Utilities

- **Extract Study IDs with Biosamples**:
  - Extracts Study GOLD IDs from the Sequencing Project sheet where Biosample GOLD IDs exist
  - Located in `sample_annotator/file_utils/extract_study_ids_with_biosamples.py`
  - Usage: `make -f make-gold-cache.Makefile extract-study-ids-with-biosamples`
  - Outputs to stdout or file (`local/gold-study-ids-with-biosamples.txt`)
  - Useful for targeting studies with associated biosamples for more selective data acquisition

## Implementation Details

### Caching System
- This project uses `diskcache` (not `requests-cache`) for API responses
- Cache implementation:
  - Located in `sample_annotator/clients/gold_client.py`
  - Cache directory is hardcoded as `CACHEDIR = "cachedir"` in the project root
  - Cache is used via decorators: `@cache.memoize()` on the `_fetch_url` function
  - No built-in way to customize cache location without code changes
  - Cache is cleared with `--purge-diskcache` flag which calls `gc.clear_cache()`

### MongoDB Connection
- The GOLD to MongoDB pipeline has been enhanced to support remote authenticated connections
- Connection options:
  - **Local unauthenticated**: Default behavior with no additional parameters
  - **Local with URI**: Use `--mongo-uri "mongodb://localhost:27017/"`
  - **Remote authenticated**: 
    - Via .env: Create `local/.env` with MongoDB credentials
    - Via command line: `--mongo-uri "mongodb://user:pass@hostname:27017/?authSource=admin"`
- A command like this will work for local connection:
  ```
  poetry run python sample_annotator/gold_to_mongo.py \
      --authentication-file config/gold-key.txt \
      --mongo-db-name gold_metadata \
      --mongo-uri "mongodb://localhost:27017/" \
      --purge-diskcache \
      --purge-mongodb \
      --study-ids-file local/gold-study-ids-with-biosamples.txt
  ```
- The `--authentication-file` is for GOLD API, not MongoDB

## Code Usage Analysis

### Actively Used Code
1. **Modules with CLI entry points** (in pyproject.toml):
   - `sample_annotator.py` - Used as CLI entry point (`annotate-sample`)
   - `gold_tool.py` - Used as CLI entry point (`gold-tool`)
   - `rel_to_oxygen_example.py` - Used as CLI entry point
   - `file_utils/extract_study_ids_with_biosamples.py` - Used as CLI entry point

2. **Modules with test coverage**:
   - Most modules with corresponding test files in the tests directory
   - GOLD API functionality (tested in `test_gold.py` and `test_gold_nmdc_pipeline.py`)
   - Annotation functionality (tested in `test_annotate.py`)
   - Measurements functionality (tested in `test_measurements.py`)

3. **Modules referenced in Makefiles**:
   - Various GOLD API functionality for data fetching and MongoDB integration

### Potentially Unused Code
1. `sample_utils.py` - Only referenced in documentation (sphinx), no imports or test coverage
2. `sample_annotator/clients/nmdc/gold_paired_end.py` - No direct imports found
3. `sample_annotator/clients/nmdc/runtime_api_client.py` - No references found
4. `sample_annotator/clients/src/` directory - Contains unused files:
   - `main.py` - No references
   - `clients/submission_portal_client.py` - No references
5. `sample_annotator/text_mining/TextMining.py` - No references found
6. `sample_annotator/ontology/Ontology.py` - No references found

### Recently Removed Modules
1. `gold_client.py` - Marked as deleted in git status, replaced by `gold_tool.py`
2. `xlsx_to_tsv.py` - Marked as deleted in git status

Note: The repository appears to be in transition, with some modules recently removed and their functionality migrated to newer modules.

## Package Distribution and Reuse

This project is fully configured for packaging and can be reused in other repositories:

### PyPI Packaging Configuration
- Complete Poetry setup for PyPI distribution in `pyproject.toml`
- Project metadata includes name, version, description, authors, and license
- Required dependencies are correctly specified
- Entry points defined in `tool.poetry.scripts` section
- Build system configured to use Poetry Core
- Test builds can be created with `poetry build` (creates wheel in dist/)
- No PyPI publishing workflow is currently set up in GitHub Actions

### Reuse Options
For integrating functionality in other projects:

1. **Direct Installation**:
   - From GitHub: `poetry add git+https://github.com/microbiomedata/sample-annotator.git`
   - From local clone: `poetry add path/to/sample-annotator`

2. **Git Integration**:
   - Fork repository for extensive modifications
   - Add as git submodule: `git submodule add https://github.com/microbiomedata/sample-annotator.git`

3. **Component Extraction**:
   - Copy specific modules like `gold_to_mongo.py` and its dependencies
   - Adjust imports as needed for your project structure
   - This approach requires manual maintenance of copied code