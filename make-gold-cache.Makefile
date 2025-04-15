# review and fix readmes and poetry dependencies

MAX_STUDIES=70000 # 2025-01

.PHONY: load-gold-biosamples-into-mongo

gold-to-mongo-all: gold-to-mongo-clean local/gold-study-ids-with-biosamples.txt

gold-to-mongo-clean:
	rm -rf downloads/goldData.xlsx local/gold-study-ids-with-biosamples.txt

downloads/goldData.xlsx:
	wget -O $@ "https://gold.jgi.doe.gov/download?mode=site_excel"

local/gold-studies.tsv: downloads/goldData.xlsx
	poetry run xlsx-to-tsv \
		--excel-file $< \
		--sheet-name Study \
		--output-file $@

# Extract Study GOLD IDs that have associated Biosample GOLD IDs
local/gold-study-ids-with-biosamples.txt: downloads/goldData.xlsx
	date && time poetry run extract-study-ids-with-biosamples \
		--excel-file $< \
		--sheet-name 'Sequencing Project' \
		--output-file $@.tmp && date # 8 minutes
	sort $@.tmp | uniq > $@
	rm -rf $@.tmp


  #  Supports both local and remote MongoDB servers with authentication.
  #
  #  Environment variables (from .env file or system):
#  MONGODB_USER: MongoDB username
#  MONGODB_PASSWORD: MongoDB password
#  MONGODB_HOST: MongoDB host (default: localhost)
#  MONGODB_PORT: MongoDB port (default: 27017)
#  MONGODB_AUTH_SOURCE: Authentication database (default: admin)
#  MONGODB_AUTH_MECHANISM: Authentication mechanism (default: SCRAM-SHA-256)

load-gold-biosamples-into-mongo: local/gold-study-ids-with-biosamples.txt
	# 		--purge-mongodb
	# 		--purge-diskcache
	poetry run gold-to-mongo \
		--authentication-file config/gold-key.txt \
		--env-file local/.env \
		--mongo-db-name gold_metadata \
		--mongo-uri "mongodb://localhost:27017/" \
		--purge-diskcache \
		--purge-mongodb \
		--study-ids-file $<

####

local/gold-cache.json: local/gold-studies.tsv
	# ~ 3 seconds/uncached study
	# GOLD has ~ 63k studies
	# < 2 days to fetch all studies ?
	poetry run python sample_annotator/clients/gold_client.py \
		--verbose \
		fetch-studies \
		--output-format json \
		--output $@ \
		--include-biosamples \
		--authentication-file config/gold-key.txt \
		$<

#.PHONY: split-out-gold-biosamples
#split-out-gold-biosamples: local/gold-cache.json
#	poetry run python sample_annotator/file_utils/split_out_gold_biosamples.py \
#		--input-file $< \
#		--study-output-file local/gold-studies-only.json \
#		--biosample-output-file local/gold-biosamples-only.json \
#		--project-output-file local/gold-projects-only.json \
#		--remove-contacts \
#		--remove-nulls