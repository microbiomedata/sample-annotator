RUN = poetry run
# SESSION_COOKIE? See https://github.com/microbiomedata/sample-annotator/issues/90
SESSION_COOKIE = $(shell cat local/SESSION_COOKIE.txt)
NMDC_SCHEMA_PATH = /Users/MAM/Documents/gitrepos/nmdc-schema/src/schema/nmdc.yaml

biosample_sqlite_file = ~/biosample_basex_data_good_subset.db

.PHONY: test clean all

all: clean test examples/outputs/report.tsv assets/bibo_DocumentStatus.tsv rel_to_oxygen_example \
examples/outputs/non_attribute_metadata_sel_envs_partial.tsv

# ---------------------------------------
# Test runner
# ----------------------------------------
test:
	$(RUN) pytest 2>&1 | tee logs/tests.log
	$(RUN) pytest -sv tests/test_capitalization.py


clean:
	find examples -name "*report.tsv" -exec rm -rf {} \;
	rm -rf assets/*bak
	rm -rf assets/*tsv
	rm -rf bin/*jar
	rm -rf downloads/*db
	rm -rf downloads/*gz
	rm -rf downloads/*owl
	rm -rf downloads/*tsv
	rm -rf downloads/*zip
	rm -rf examples/outputs/*tsv
	rm -rf logs/*log
	rm -rf target/*

examples/outputs/report.tsv: examples/gold.json
	$(RUN) annotate-sample -R $@ $<

downloads/mixs6_core.tsv:
	curl -L -s 'https://docs.google.com/spreadsheets/d/1QDeeUcDqXes69Y2RjU2aWgOpCVWo5OVsBX9MKmMqi_o/export?format=tsv&gid=178015749' > $@

examples/outputs/non_attribute_metadata_sel_envs_partial.tsv:
	$(RUN) sqlite_client_cli \
		--sqlite_path $(biosample_sqlite_file) \
		--query "select * from non_attribute_metadata_sel_envs limit 9" \
		--tsv_out $@

rel_to_oxygen_example: downloads/mixs6_core.tsv
	$(RUN) rel_to_oxygen_example \
		--sqlite_path $(biosample_sqlite_file) \
		--mixs_core_path $<


bin/robot.jar:
	curl -s https://api.github.com/repos/ontodev/robot/releases/latest  | grep 'browser_download_url.*\.jar"' |  cut -d : -f 2,3 | tr -d \" | wget -O $@ -i -

downloads/bibo.owl:
	# --location (-L) pursues redirects
	curl --location https://raw.githubusercontent.com/structureddynamics/Bibliographic-Ontology-BIBO/master/bibo.owl -o $@

assets/bibo_DocumentStatus.tsv: downloads/bibo.owl bin/robot.jar
	java -jar bin/robot.jar query --input $< --query sparql/bibo_DocumentStatus.sparql $@
	sed --in-place=.bak 's/^\?//' $@

.PHONY: clean_loosies
clean_loosies:
	rm -rf assets/out/*json
	rm -rf assets/out/*tsv
	rm -rf assets/out/*yaml
	rm -rf bs_db.json instantiation_log.yml submission_frame.tsv sample_data.tsv

# todo delete
#assets/out/biosample_collection.json: $(NMDC_SCHEMA_PATH) clean_loosies
#	$(RUN) python sample_annotator/clients/nmdc/get_metadata_submissions.py \
#		--session_cookie $(SESSION_COOKIE) \
#		--study_id "cc498964-d1da-416d-b353-aecf5f6c749d"

assets/out/submissions_as_studies.yaml:
	$(RUN) python sample_annotator/clients/nmdc/submissions_to_nmdc_databases.py \
		--merge_known_orcids False

assets/out/biosmaples_tsv_to_json.json:
	$(RUN) python sample_annotator/clients/nmdc/biosamples_tsv_to_json.py \
		--csv_in /Users/MAM/Bioscales_NMDC_import_nospace_temps.csv \
		--yaml_out /Users/MAM/Bioscales_NMDC_import_nospace_temps.yaml \
		--asserted_template bioscales \
		--asserted_study study1234
  #  --infer / --no-infer            Infer missing slot values  [default: no-
  #                                  infer]

# todo if including --module /Users/MAM/Documents/gitrepos/nmdc-schema/nmdc_schema/nmdc.py
#   KeyError: 'applies to person'
assets/out/submissions_as_studies.json: assets/in/study_database_bottomup.yaml
	$(RUN) linkml-validate \
		--target-class Database \
		--index-slot study_set \
		--schema /Users/MAM/Documents/gitrepos/nmdc-schema/src/schema/nmdc.yaml $<
	$(RUN) linkml-convert \
		--output $@ \
		--target-class Database \
		--index-slot study_set \
		--schema /Users/MAM/Documents/gitrepos/nmdc-schema/src/schema/nmdc.yaml \
		--module /Users/MAM/Documents/gitrepos/nmdc-schema/nmdc_schema/nmdc.py $<


# usages of sample_annotator/clients/nmdc/api_or_tsv_metadata_submissions_to_json.py

# NEW BIOSAMPLE INSTANTIATION

.PHONY: api_or_tsv_clean

PLACEHOLDER_TEXT="forces creation of this directory"

api_or_tsv_clean:
	rm -rf assets/out/*
	mkdir -p assets/out
	echo $(PLACEHOLDER_TEXT) > assets/out/README.md


# todo this errors out if 0 valid biosample rows are found
#  do be careful with start and stop values (0-1 is a failure case?)
#  these are pages of submissions
#  2022-08-09 2-4 works nicely
#  0-8, 10-11 OK
#  9-9 picks up some soil horizon problems
# 12 problems with sample_type and samp_collec_device:russiancorer

assets/out/sample_metadata_from_api.yaml: api_or_tsv_clean
	# --data_portal_url https://data.dev.microbiomedata.org/ or https://data.microbiomedata.org/
	$(RUN) api_or_tsv_metadata_submissions_to_json from-submissions \
		--data_portal_url https://data.microbiomedata.org/ \
		--page_start 0 \
		--page_stop 999 \
		--sample_metadata_csv_file $(basename $@).csv  \
		--sample_metadata_yaml_file $@ \
		--study_metadata_yaml_file $(subst sample,study,$(basename $@)).yaml 2> assets/out/api_or_tsv_metadata_submissions_to_json.log
	# todo refactor everything below
	$(RUN) linkml-validate \
		--target-class Database \
		--index-slot biosample_set \
		--schema $(PROJ_SCHEMA) $@
	$(RUN) linkml-convert \
		--target-class Database \
		--module /Users/MAM/Documents/gitrepos/nmdc-schema/python/nmdc.py \
		--schema /Users/MAM/Documents/gitrepos/nmdc-schema/src/schema/nmdc.yaml \
		--output $(basename $@).json $@
	# todo why is this @type removal required?
	# todo where did it come from?
	jq 'del(."@type")' $(basename $@).json > $(basename $@)_untyped.json
	$(RUN) jsonschema -i $(basename $@)_untyped.json /Users/MAM/Documents/gitrepos/nmdc-schema/jsonschema/nmdc.schema.json



PROJ_BIOSAMPLE_CSV=/Users/MAM/Documents/Bioscales_NMDC_import_nospace_temps.csv
PROJ_ID_FOR_CSV='gold:Gs0154044'
PROJ_DH_TEMPLATE=bioscales
PROJ_SCHEMA=/Users/MAM/Documents/gitrepos/nmdc-schema/src/schema/nmdc.yaml

# todo clarify that this is DH CSV
# todo switch from csv to tsv or auto-sense
assets/out/sample_metadata_from_csv.yaml: api_or_tsv_clean
	$(RUN) api_or_tsv_metadata_submissions_to_json from-csv \
		--data_csv $(PROJ_BIOSAMPLE_CSV) \
		--sample_metadata_csv_file $(basename $@).csv \
		--static_dh_template $(PROJ_DH_TEMPLATE) \
		--static_project_id $(PROJ_ID_FOR_CSV) \
		--sample_metadata_yaml_file $@
	$(RUN) linkml-validate \
		--target-class Database \
		--index-slot biosample_set \
		--schema $(PROJ_SCHEMA) $@
	$(RUN) linkml-convert \
		--target-class Database \
		--module /Users/MAM/Documents/gitrepos/nmdc-schema/python/nmdc.py \
		--schema /Users/MAM/Documents/gitrepos/nmdc-schema/src/schema/nmdc.yaml \
		--output $(basename $@).json $@
	# todo why is this @type removal required?
	# todo where did it come from?
	jq 'del(."@type")' $(basename $@).json > $(basename $@)_untyped.json
	$(RUN) jsonschema -i $(basename $@)_untyped.json /Users/MAM/Documents/gitrepos/nmdc-schema/jsonschema/nmdc.schema.json

# m-cafes
# BIOPROJECT:PRJNA692505
PROJ_SQLITE_FILE = /Users/MAM/Downloads/biosample_basex.db
# not to be confused with biosample_sqlite_file
PROJ_ACCESSIONS_FILE = assets/in/mcafe_accessions.txt
PROJ_ID_FOR_SQLITE='gold:Gs0110119'

# todo click options should always be hyphen seperated not underscore seperated


assets/out/sample_metadata_from_pure_sqlite.yaml: api_or_tsv_clean
	$(RUN) api_or_tsv_metadata_submissions_to_json pure-from-sqlite \
		--biosample-sql-file $(PROJ_SQLITE_FILE) \
		--biosample-id-file $(PROJ_ACCESSIONS_FILE) \
		--static-project-id $(PROJ_ID_FOR_SQLITE) \
		--sqlite-to-biosample-file assets/in/sqlite_to_v3_plus_v6.tsv \
		--sample-metadata-yaml-file $@

	cat $(basename $@).json | \
		jq 'del(."@type")'  > \
		$(basename $@)_no_outer_type.json

	$(RUN) jsonschema -i $(basename $@)_no_outer_type.json /Users/MAM/Documents/gitrepos/nmdc-schema/jsonschema/nmdc.schema.json

	# todo refactor from here down

	cat $(basename $@).json | \
		jq 'del(."@type")' | \
		jq 'del(.biosample_set[].sample_link)' | \
		jq 'del(.biosample_set[].project_ID)' > \
		$(basename $@)_no_outer_type_no_v6_only.json

	# create v3-like depths and depth2s from v6 depths
	#   tried but failed to do this in jq
	$(RUN) python sample_annotator/clients/nmdc/depth_fix_for_v3.py \
		--input_json_file $(basename $@)_no_outer_type_no_v6_only.json \
		--output_json_file $(basename $@)_v3_with_depth2.json
	# todo validate against v3 !

assets/out/sample_metadata_from_pure_gold_api.yaml: api_or_tsv_clean
	$(RUN) api_or_tsv_metadata_submissions_to_json pure-from-gold-study \
			--gold-study-id "gold:Gs0110119" \
			--gold-mapping-file assets/in/gold_nmdc_mapping.tsv \
			--sample-metadata-yaml-file $@

	cat $(basename $@).json | \
		jq 'del(."@type")'  > \
		$(basename $@)_no_outer_type.json

	$(RUN) jsonschema -i $(basename $@)_no_outer_type.json /Users/MAM/Documents/gitrepos/nmdc-schema/jsonschema/nmdc.schema.json

	# todo refactor from here down

	cat $(basename $@).json | \
		jq 'del(."@type")' | \
		jq 'del(.biosample_set[].sample_link)' | \
		jq 'del(.biosample_set[].project_ID)' > \
		$(basename $@)_no_outer_type_no_v6_only.json

	# create v3-like depths and depth2s from v6 depths
	#   tried but failed to do this in jq
	$(RUN) python sample_annotator/clients/nmdc/depth_fix_for_v3.py \
		--input_json_file $(basename $@)_no_outer_type_no_v6_only.json \
		--output_json_file $(basename $@)_v3_with_depth2.json
	# todo validate against v3 !

assets/out/sample_metadata_from_hybrid.yaml: api_or_tsv_clean
	$(RUN) api_or_tsv_metadata_submissions_to_json sqlite-gold-hybrid \
			--biosample-id-file $(PROJ_ACCESSIONS_FILE) \
			--biosample-sql-file $(PROJ_SQLITE_FILE) \
			--gold-mapping-file assets/in/gold_nmdc_mapping.tsv \
			--gold-study-id "gold:Gs0110119" \
			--lookup_file /Users/MAM/Documents/Angelo_2014-Banfield-JGI-data.tsv \
			--lookup_style mcafes_gold_lookup \
			--sample-metadata-yaml-file $@ \
			--sqlite-to-biosample-file assets/in/sqlite_to_v3_plus_v6.tsv

	cat $(basename $@).json | \
		jq 'del(."@type")'  > \
		$(basename $@)_no_outer_type.json

	$(RUN) jsonschema -i $(basename $@)_no_outer_type.json /Users/MAM/Documents/gitrepos/nmdc-schema/jsonschema/nmdc.schema.json

	cat $(basename $@).json | \
		jq 'del(."@type")' | \
		jq 'del(.biosample_set[].sample_link)' | \
		jq 'del(.biosample_set[].project_ID)' > \
		$(basename $@)_no_outer_type_no_v6_only.json
