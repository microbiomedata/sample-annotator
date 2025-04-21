# NMDC-GOLD Metadata Ingestion and Management Pipeline

## Overview

This document provides a complete technical overview of how metadata from the **Genomes Online Database (GOLD)** is ingested, cached, and structured for use within the **National Microbiome Data Collaborative (NMDC)**. This pipeline was developed to overcome key limitations in GOLD's available interfaces and formats, enabling:

- **Scalable bulk download**
- **Real-time access and Boolean filtering**
- **Metadata enrichment and derived value storage**
- **Cross-deployment compatibility (local + NERSC)**

---

## Original Interfaces and Their Limitations

### 🖥️ GOLD Website

- URL: [https://gold.jgi.doe.gov/](https://gold.jgi.doe.gov/)
- Offers filtering on select combinations of fields.
- **Limitations**:
  - No ability to filter using Boolean OR across arbitrary fields.
  - No programmatic interface or saved query system.
  - Result sets are limited in size and only downloadable via form submission.

---

### 📊 Public Excel File (`goldData.xlsx`)

- Link: [https://gold.jgi.doe.gov/download?mode=site_excel](https://gold.jgi.doe.gov/download?mode=site_excel)
- Downloaded file name: `goldData.xlsx`
- Tabs:
  - `Readme`
  - `Study`
  - `Biosample`
  - `Organism`
  - `Sequencing Project`
  - `Analysis Project`

#### Issues:
- ~200MB file with 200k+ rows in the `Biosample` tab alone.
- **Performance**: Extremely slow on non-Excel tools (e.g., LibreOffice on Linux).
- **Missing Data**: Critical metadata fields such as the MIxS environmental context fields (`env_broad_scale`, `env_local_scale`, `env_medium`) are **not present**.
- **No incremental updates**, no provenance/versioning, and no schema documentation.

---

### 🧩 GOLD Swagger API

- Swagger UI: [https://gold-ws.jgi.doe.gov/swagger-ui/index.html](https://gold-ws.jgi.doe.gov/swagger-ui/index.html)
- Provides JSON access for metadata via `/biosamples`, `/studies`, etc.

#### Key Limitation:
The `/biosamples` endpoint only supports queries where a **single ID** (like `studyGoldId` or `projectGoldId`) is provided:
```json
GET /biosamples?studyGoldId=Gs0000008
```

- No filtering by metadata fields (e.g., ecosystem, location).
- No pagination.
- No bulk download of all biosamples or studies.

---

### 🔐 NMDC-specific GOLD API

- Base URL: `https://gold.jgi.doe.gov/rest/nmdc`
- Authenticated using NMDC-shared credentials (via HTTP Basic Auth).
- Documentation: [Google Doc](https://docs.google.com/document/d/1PgrFYmc7AU7Kd5Dtg-xbpAyC6ZcLw4ChFwg3bHV1JQg/edit?tab=t.0)

#### Excerpted endpoints:
```
GET /rest/nmdc/biosamples?studyGoldId=Gs0114675
GET /rest/nmdc/biosamples?itsProposalId=1777
```

- Same ID-based restriction as public API.
- No flexible metadata filtering.
- Not documented or supported publicly.

---

## Project Goals and Tools

> _"I want to be realistic about the different forms of data serialization we need (monolithic file, MongoDB, and diskcache) and what inter-conversions we need."_  
> — You

---

### Goals

- Avoid maintaining both `gold_tool.py` and legacy `gold_cache`.
- Standardize around a single client (`gold_tool.py`) that supports:
  - API ingestion
  - Diskcache population
  - MongoDB storage
  - Interconversion
- Operable from:
  - Local workstation
  - NERSC Perlmutter
- With target MongoDB on:
  - `mongo-ncbi-loadbalancer.mam.production.svc.spin.nersc.org`

---

## Architecture

### 1. Study ID Acquisition

- Download or curate list of `Gs...` study IDs.
- Store in `local/gold-study-ids-with-biosamples.txt`.

---


### 2. Ingestion Options: Legacy vs Unified Tool

#### 🧪 Legacy Tool: `sample_annotator/clients/gold_client.py`

This earlier pipeline, developed by Chris Mungall, downloads selected GOLD study metadata into a **monolithic JSON file** (`local/gold-cache.json`), using `diskcache` as a fallback during retrieval. It does **not** write directly to MongoDB.

**Sample Makefile target**:
```make
local/gold-cache.json: local/gold-studies.tsv
	# ~3 seconds/study → ~2.5 days for 63k studies
	$(RUN) python sample_annotator/clients/gold_client.py \
		--verbose \
		fetch-studies \
		--output-format json \
		--output $@ \
		--include-biosamples \
		--authentication-file config/gold-key.txt \
		$<
```

While still usable, this tool is **no longer preferred**, as it separates data fetching from persistence and is tied to file-based outputs.

---

#### ✅ Unified Tool: `gold_tool.py`

Your newer tool combines:
- Credential handling
- Diskcache use
- Direct ingestion into MongoDB
- Optional recovery of diskcache from MongoDB
- Makefile-free CLI interface via Click

**Preferred usage**:
```bash
poetry run gold-tool load-to-mongodb \
  --study-ids-file local/gold-studies.tsv \
  --mongo-uri "mongodb://localhost:27017/gold_metadata" \
  --authentication-file config/gold-key.txt \
  --env-file local/.env \
  --resume \
  --batch-size 100 \
  --max-retries 3
```

This tool is now canonical and supports the **entire ingestion-to-MongoDB pipeline** in a single flow, while also supporting interconversion back to diskcache and forward to post-processing.


- Unified Python client that:
  - Loads credentials from a file
  - Uses `diskcache` to memoize API responses
  - Streams studies, biosamples, and sequencing projects into MongoDB

#### Sample Makefile entry (from `sample-annotator`):
```make
local/gold-cache.json: local/gold-studies.tsv
	# ~ 3 seconds/uncached study
	# GOLD has ~ 63k studies
	# ~ 2.5 days to fetch all studies with no hiccups
	$(RUN) python sample_annotator/clients/gold_client.py \
		--verbose \
		fetch-studies \
		--output-format json \
		--output $@ \
		--include-biosamples \
		--authentication-file config/gold-key.txt \
		$<
```

---

### 3. MongoDB Structure

| Collection | Key Field | Notes |
|------------|-----------|-------|
| `studies` | `studyGoldId` | References biosamples |
| `biosamples` | `biosampleGoldId` | May contain `projects` inline |
| `seq_projects` | `projectGoldId` | Linked via `biosampleGoldId` |
| `study_import_failures` | `studyGoldId` | Contains error trace and timestamp |

---

### 4. Interconversion and Recovery


### MongoDB-to-MongoDB Transfer as a Data Portability Use Case

While interconversion typically refers to switching data formats (e.g., from diskcache to MongoDB, or from MongoDB to monolithic JSON), **migrating records between MongoDB deployments** is a related concern in this system.

These transfers are not format changes, but they are **critical to deployment flexibility** and share the same goals:
- Preservation of document structure and indexes
- Ensuring that derived and enriched metadata are retained
- Allowing re-ingestion, reannotation, and analysis in a new environment

Common scenario:
- The canonical GOLD data is loaded into a **MongoDB running on NERSC SPIN**.
- A full or partial export of those collections is copied to a **MongoDB on a local workstation**, allowing offline work or debugging.
- This transfer must maintain consistency with associated diskcache files, or regenerate them if needed.

As such, **MongoDB-to-MongoDB migration is considered part of the overall data flow strategy**, alongside format-level conversions.




| From | To | Method |
|------|----|--------|
| MongoDB → diskcache | `gold_tool.py rebuild-cache` |
| diskcache → MongoDB | `gold_tool.py load-to-mongodb` (uses cache) |
| MongoDB → JSON | via separate exporter or postprocessor |
| API → diskcache → MongoDB | main ingestion route |
| JSON → MongoDB | only if dump created previously |

---

### 5. Post-processing (`external-metadata-awareness`)

After MongoDB loading, data is enriched with:
- MIxS environmental triads
- Normalized ontology terms
- Parsed units and quantities

This phase is out-of-scope for `gold_tool.py` but essential to downstream NMDC pipelines.

---

## Runtime and Portability

- Tools run on:
  - NERSC Perlmutter login nodes
  - Local Ubuntu or macOS workstation
- MongoDB can be local or remote (NERSC SPIN or elsewhere)

---

## Migration Strategy

- When migrating MongoDBs:
  - **Download from NERSC → Home** is faster than reverse (due to asymmetric bandwidth)
- Diskcache helps resume operations after crashes without redundant API calls

---

## Summary

This system addresses major gaps in GOLD’s data access methods by:

- **Unifying ingestion, caching, and storage**
- **Enabling Boolean and cross-field filtering**
- **Supporting both structured and enriched metadata**
- **Providing failover and recovery mechanisms**

---


### 6. Post-Ingestion Enhancement of GOLD Records

After GOLD metadata is loaded into MongoDB, the `external-metadata-awareness` repository provides specialized tools to flatten and enhance the biosample records. These transformations make the metadata more queryable, ontology-aligned, and suitable for downstream analysis.

---

#### 🧩 Primary Flattening Script

**1. `insert_all_flat_gold_biosamples.py`**
- Command-line tool with flexible MongoDB connection options
- Flattens GOLD biosamples into a standardized tabular format
- Adds value by:
  - Converting environmental IDs to proper CURIEs (e.g., `ENVO_01000339` → `ENVO:01000339`)
  - Looking up canonical labels using ontologies
  - Flagging obsolete ontology terms
  - Adding MIxS-style standardized field names for environmental triads

**2. Makefile Command**
```bash
make -f Makefiles/gold.Makefile flatten-gold-biosamples
```
- Runs the flattening script with preconfigured MongoDB settings
- Parameters (e.g., host, port, db name) can be overridden via `.env` or CLI

---

#### 🔬 Flattening Process Features

**1. Ontology Integration**
- Uses OAK (Ontology Access Kit) to load ENVO, PO, and UBERON
- Constructs efficient label caches for term lookups
- Detects and flags obsolete ontology terms

**2. Environmental Context Enhancement**
- Adds canonical CURIEs and labels for environmental triads:
  - `env_broad_scale_canonical_curie`
  - `env_broad_scale_canonical_label`
  - `env_local_scale_canonical_curie`
  - `env_local_scale_canonical_label`
  - `env_medium_canonical_curie`
  - `env_medium_canonical_label`
- Adds boolean fields:
  - `env_broad_scale_is_obsolete`
  - `env_local_scale_label_mismatch`
  - Etc.

**3. Contact Information Extraction**
- Extracts contact info from nested GOLD biosample records
- Populates a separate `biosample_contacts` collection
- Standardizes contact roles (e.g., submitter, PI)

**4. Data Cleanup**
- Removes root-level metadata keys that provide no analytical value
- Handles:
  - Lists of scalars → joined with pipes (`|`)
  - Nested dictionaries → flattened into dotted keys
  - Scalars → passed through unmodified

---

This enrichment step transforms raw GOLD records into enriched MIxS-compatible biosamples with normalized ontology terms, structured contacts, and highly regular formats for downstream workflows.


# Primary Flattening Scripts

1. **`insert_all_flat_gold_biosamples.py`**
   - Command-line tool with flexible MongoDB connection options
   - Flattens GOLD biosamples into a more accessible tabular structure
   - Adds value by:
     - Converting environmental identifiers to proper CURIE format (e.g., `ENVO_01000339` → `ENVO:01000339`)
     - Looking up canonical ontology labels
     - Flagging obsolete ontology terms
     - Creating MIxS-style standardized fields for environmental triads

2. **Makefile Command**
   ```bash
   make -f Makefiles/gold.Makefile flatten-gold-biosamples
   ```
   - Wraps the above script using environment-aware settings
   - MongoDB connection parameters can be overridden via `.env`

---

#### Flattening Process Features

1. **Ontology Integration**
   - Leverages OAK (Ontology Access Kit) to load ENVO, PATO, UBERON, and others
   - Builds label caches for efficient lookups
   - Detects and flags obsolete ontology terms

2. **Environmental Context Enhancement**
   - Adds standardized MIxS-style fields:
     - `env_broad_scale_canonical_curie`
     - `env_broad_scale_canonical_label`
     - `env_local_scale_canonical_curie`
     - `env_local_scale_canonical_label`
     - `env_medium_canonical_curie`
     - `env_medium_canonical_label`
   - Adds boolean flags for:
     - Term obsolescence
     - Label mismatches between asserted and canonical values

3. **Contact Information Extraction**
   - Extracts submitter, investigator, and other contact roles into a dedicated collection
   - Normalizes contact fields (name, email, role)

4. **Data Cleanup**
   - Removes redundant or empty root-level keys
   - Coerces scalar values, lists, and dictionaries into uniform formats
   - Joins scalar list values with pipe (`|`) delimiters

---

These transformations are critical for enabling high-quality search, validation, and downstream integration of GOLD-derived biosample metadata in NMDC workflows.


---

## 📌 Notes

- This documentation file will be saved in both:
  - [https://github.com/microbiomedata/sample-annotator](https://github.com/microbiomedata/sample-annotator)
  - [https://github.com/microbiomedata/external-metadata-awareness](https://github.com/microbiomedata/external-metadata-awareness)

- The issue of whether **empty fields** (e.g., nulls or empty strings) should be retained in MongoDB documents for schema consistency—or omitted entirely from derived or flattened documents—**has not been resolved consistently**. Future work should clarify and document the preferred policy.
