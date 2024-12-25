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

local/gold-study-ids-10.txt: local/gold-study-ids.txt
	head -n 10 $< > $@

local/gold-cache.yaml: local/gold-study-ids-10.txt
	poetry run python sample_annotator/clients/gold_client.py \
		--verbose \
		fetch-studies \
		--output-format yaml \
		--output $@ \
		--include-biosamples \
		--authentication-file config/gold-key.txt \
		$<