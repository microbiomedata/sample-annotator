# review and fix readmes and poetry dependencies

# see also https://github.com/microbiomedata/external-metadata-awareness/blob/751ddb6360f95f164a6605ca056e81fced59e195/Makefiles/gold.Makefile

RUN=poetry run

.PHONY: gold-to-mongo-all gold-to-mongo-clean load-gold-biosamples-into-mongo rebuild-gold-cache-from-mongodb

gold-to-mongo-all: gold-to-mongo-clean load-gold-biosamples-into-mongo

gold-to-mongo-clean:
	rm -rf downloads/goldData.xlsx local/gold-study-ids-with-biosamples.txt

downloads/goldData.xlsx:
	curl -o $@ "https://gold.jgi.doe.gov/download?mode=site_excel"

local/gold-studies.tsv: downloads/goldData.xlsx
	$(RUN) xlsx-to-tsv \
		--excel-file $< \
		--sheet-name Study \
		--output-file $@

# Extract Study GOLD IDs that have associated Biosample GOLD IDs
local/gold-study-ids-with-biosamples.txt: downloads/goldData.xlsx
	date && time $(RUN) extract-study-ids-with-biosamples \
		--excel-file $< \
		--sheet-name 'Sequencing Project' \
		--output-file $@.tmp && date # 8 minutes
	sort $@.tmp | uniq > $@
	rm -rf $@.tmp

#  gold-to-mongo no supports both local and remote MongoDB servers with or without authentication.
#
#  Environment variables (from .env file)
#  MONGODB_USER: MongoDB username
#  MONGODB_PASSWORD: MongoDB password

#	# 		--purge-mongodb
#	# 		--purge-diskcache
#	# 		--env-file

#load-gold-biosamples-into-mongo: local/gold-study-ids-with-biosamples.txt
#	$(RUN) gold-to-mongo \
#		--authentication-file config/gold-key.txt \
#		--cache-dir cache_from_local_mongodb \
#		--env-file local/.env.27778 \
#		--log-failures-to-file local/gold-to-mongo-failures.json \
#		--mongo-uri "mongodb://localhost:27778/staging?directConnection=true&authMechanism=DEFAULT&authSource=admin" \
#		--study-ids-file $<


load-gold-biosamples-into-mongo: local/gold-study-ids-with-biosamples.txt
	$(RUN) gold-to-mongo \
		--authentication-file config/gold-key.txt \
		--cache-dir cache_from_local_mongodb \
		--log-failures-to-file local/gold-to-mongo-failures.json \
		--mongo-uri "mongodb://localhost:27017/gold_metadata_2" \
		--study-ids-file $<

rebuild-gold-cache-from-mongodb:
	poetry run rebuild-gold-cache \
		--mongo-uri "mongodb://localhost:27017/gold_metadata" \
		--authentication-file config/gold-key.txt \
		--cache-dir cache_from_local_mongodb


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

