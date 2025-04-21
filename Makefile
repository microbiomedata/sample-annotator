RUN = poetry run

.PHONY: test clean all

all: clean test examples/outputs/report.tsv

# ---------------------------------------
# Test runner
# ----------------------------------------
test:
	$(RUN) pytest 2>&1 | tee logs/tests.log

clean:
	find examples -name "*report.tsv" -exec rm -rf {} \;
	rm -rf logs/*log

examples/outputs/report.tsv: examples/gold.json
	$(RUN) annotate-sample -R $@ $<

# review and fix readmes and poetry dependencies

# see also https://github.com/microbiomedata/external-metadata-awareness/blob/751ddb6360f95f164a6605ca056e81fced59e195/Makefiles/gold.Makefile

.PHONY: gold-to-mongo-all gold-to-mongo-clean load-gold-biosamples-into-mongo rebuild-gold-cache-from-mongodb

gold-to-mongo-all: gold-to-mongo-clean load-gold-biosamples-into-mongo

gold-to-mongo-clean:
	rm -rf downloads/goldData.xlsx local/gold-study-ids-with-biosamples.txt local/gold-study-ids-10.tsv local/gold-studies.tsv

downloads/goldData.xlsx:
	curl -o $@ "https://gold.jgi.doe.gov/download?mode=site_excel"

local/gold-studies.tsv: downloads/goldData.xlsx
	$(RUN) xlsx-to-tsv \
		--excel-file $< \
		--sheet-name Study \
		--output-file $@

local/gold-study-ids-10.tsv: local/gold-studies.tsv
	head -n 10 $< | tail -n +2 | cut -f1 > $@

# Extract Study GOLD IDs that have associated Biosample GOLD IDs
local/gold-study-ids-with-biosamples.txt: downloads/goldData.xlsx
	date && time $(RUN) extract-study-ids-with-biosamples \
		--excel-file $< \
		--sheet-name 'Sequencing Project' \
		--output-file $@.tmp && date # 8 minutes
	sort $@.tmp | uniq > $@
	rm -rf $@.tmp


# (sample-annotator-py3.10) mark@mark-NUC10i7FNH:~/gitrepos/sample-annotator$ gold-tool --help
  #Usage: gold-tool [OPTIONS] COMMAND [ARGS]...
  #
  #  GOLD API client with caching and MongoDB integration.
  #
  #Options:
  #  -v, --verbose     Increase verbosity
  #  -q, --quiet       Decrease verbosity
  #  --cache-dir TEXT  Path to cache directory
  #  --help            Show this message and exit.
  #
  #Commands:
  #  fetch-studies    Fetch studies from GOLD API and save as JSON or YAML.
  #  inspect-cache    Print information about cache contents.
  #  load-to-mongodb  Load GOLD data into MongoDB.
  #  rebuild-cache    Rebuild cache from MongoDB data.

# gold-tool fetch-studies --help
  #2025-04-21 16:31:07,514 - INFO - Cache initialized at cachedir with 0 records
  #Usage: gold-tool fetch-studies [OPTIONS] IDFILE
  #
  #  Fetch studies from GOLD API and save as JSON or YAML.
  #
  #  IDFILE is a file containing GOLD study IDs (Gs...) one per line.
  #
  #Options:
  #  -o, --output FILENAME           Output file
  #  -O, --output-format [json|yaml]
  #                                  Output format
  #  --include-biosamples / --no-include-biosamples
  #                                  Include biosamples in study data
  #  --clear-cache / --no-clear-cache
  #                                  Clear cache before fetching
  #  -a, --authentication-file TEXT  Path to authentication file
  #  --help                          Show this message and exit.

local/gold-cache-from-gold-cache.json: local/gold-study-ids-10.tsv
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

local/gold-cache-from-gold-tool.json: local/gold-study-ids-10.tsv
	# uses different cache keys from sample_annotator/clients/gold_client.py fetch-studies
	# interleaved logging/tqdm usage could be improved
	$(RUN) gold-tool fetch-studies \
		--output $@ \
		--output-format json \
		--include-biosamples \
		--authentication-file config/gold-key.txt \
		$<

local/gold-dump-diff.txt: local/gold-cache-from-gold-cache.json local/gold-cache-from-gold-tool.json
	poetry run python sample_annotator/json_diff.py \
	  --file1 local/gold-cache-from-gold-cache.json \
	  --file2 local/gold-cache-from-gold-tool.json \
	  --show-diff > $@


#gold-tool inspect-cache --help
#2025-04-21 16:31:59,636 - INFO - Cache initialized at cachedir with 0 records
#Usage: gold-tool inspect-cache [OPTIONS]
#
#  Print information about cache contents.
#
#Options:
#  --help  Show this message and exit.

.PHONY: inspect-cache

inspect-cache:
	$(RUN) gold-tool inspect-cache


# gold-tool load-to-mongodb  --help
  #2025-04-21 16:32:44,953 - INFO - Cache initialized at cachedir with 0 records
  #Usage: gold-tool load-to-mongodb [OPTIONS]
  #
  #  Load GOLD data into MongoDB.
  #
  #  Fetches studies, biosamples, and projects from GOLD API and stores them in
  #  MongoDB collections.
  #
  #Options:
  #  -i, --study-ids-file PATH       File containing study IDs  [required]
  #  -u, --mongo-uri TEXT            MongoDB URI  [required]
  #  -e, --env-file TEXT             Environment file with MongoDB credentials
  #  -a, --authentication-file TEXT  GOLD API authentication file
  #  --log-failures-to-file PATH     Write failures to JSON file
  #  -r, --resume / --no-resume      Skip already processed studies
  #  -b, --batch-size INTEGER        Number of studies per batch
  #  -m, --max-retries INTEGER       Maximum retry attempts
  #  --help                          Show this message and exit.

# disccache purges should be performed with `rm`
# mongodb purges should be performed with db.colelction.drop()

# now supports both local and remote MongoDB servers with or without authentication.
#
#  Environment variables (from .env file)
#  MONGODB_USER: MongoDB username
#  MONGODB_PASSWORD: MongoDB password

load-gold-biosamples-into-mongo: local/gold-study-ids-10.tsv
	$(RUN) gold-tool load-to-mongodb \
		--authentication-file config/gold-key.txt \
		--log-failures-to-file local/gold-to-mongo-failures.json \
		--mongo-uri "mongodb://localhost:27017/gold_cache_for_10_studies" \
		--study-ids-file $<

#gold-tool rebuild-cache  --help
#2025-04-21 16:36:01,132 - INFO - Cache initialized at cachedir with 0 records
#Usage: gold-tool rebuild-cache [OPTIONS]
#
#  Rebuild cache from MongoDB data.
#
#  Uses existing MongoDB collections to populate cache without making API
#  calls.
#
#Options:
#  --mongo-uri TEXT                MongoDB URI including database name
#                                  [required]
#  -e, --env-file TEXT             Path to .env file with MongoDB credentials
#  -a, --authentication-file TEXT  GOLD API authentication file
#  --help                          Show this message and exit.

rebuild-gold-cache-from-mongodb:
	$(RUN) gold-tool rebuild-cache \
		--mongo-uri "mongodb://localhost:27017/gold_cache_for_10_studies" \
		--authentication-file config/gold-key.txt
