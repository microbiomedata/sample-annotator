# review and fix readmes and poetry dependencies
# use .gitkeep for keeping directories

# todo: just get environmental microbiome studies?

# in separate PRs!


downloads/goldData.xlsx:
	wget -O $@ "https://gold.jgi.doe.gov/download?mode=site_excel"

local/gold-studies.tsv: downloads/goldData.xlsx
	poetry run python sample_annotator/file_utils/xlsx_to_tsv.py \
		--excel-file $< \
		--sheet-name Study \
		--output-file $@

local/gold-study-ids.txt: local/gold-studies.tsv
	tail -n +2 $< | cut -f 1 > $@

local/gold-study-ids-1000.txt: local/gold-study-ids.txt
	head -n 1000 $< > $@

local/gold-cache.json: local/gold-study-ids-1000.txt
	poetry run python sample_annotator/clients/gold_client.py \
		--verbose \
		fetch-studies \
		--output-format json \
		--output $@ \
		--include-biosamples \
		--authentication-file config/gold-key.txt \
		$<

.PHONY: split-out-gold-biosamples
split-out-gold-biosamples: local/gold-cache.json
	poetry run python sample_annotator/file_utils/split_out_gold_biosamples.py \
		--input-file $< \
		--study-output-file local/gold-studies-only.json \
		--biosample-output-file local/gold-biosamples-only.json \
		--project-output-file local/gold-projects-only.json \
		--remove-contacts \
		--remove-nulls
