# todo get rid of embedded absolute paths

RUN = poetry run
NMDC_SCHEMA_PATH = assets/in/nmdc_schema_6/nmdc.yaml
PLACEHOLDER_TEXT="forces creation of this directory"

DH_BIOSAMPLE_CSV=assets/in/Bioscales_NMDC_import_nospace_temps.csv
PROJ_ID_FOR_CSV='gold:Gs0154044'
DH_TEMPLATE=bioscales

# m-cafes
# BIOPROJECT:PRJNA692505
BIOSAMPLE_SQLITE_FILE = /Users/MAM/Downloads/biosample_basex.db
# not to be confused with dg_biosample_sqlite_file
ACCESSIONS_FILE = assets/in/mcafe_accessions.txt
PROJ_ID_FOR_SQLITE='gold:Gs0110119'

#dg_biosample_sqlite_file = ~/biosample_basex_data_good_subset.db

.PHONY: test clean all api_or_tsv_clean

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

#examples/outputs/non_attribute_metadata_sel_envs_partial.tsv:
#	$(RUN) sqlite_client_cli \
#		--sqlite_path $(dg_biosample_sqlite_file) \
#		--query "select * from non_attribute_metadata_sel_envs limit 9" \
#		--tsv_out $@
#
#rel_to_oxygen_example: downloads/mixs6_core.tsv
#	$(RUN) rel_to_oxygen_example \
#		--sqlite_path $(dg_biosample_sqlite_file) \
#		--mixs_core_path $<


bin/robot.jar:
	curl -s https://api.github.com/repos/ontodev/robot/releases/latest  | grep 'browser_download_url.*\.jar"' |  cut -d : -f 2,3 | tr -d \" | wget -O $@ -i -

downloads/bibo.owl:
	# --location (-L) pursues redirects
	curl --location https://raw.githubusercontent.com/structureddynamics/Bibliographic-Ontology-BIBO/master/bibo.owl -o $@

assets/bibo_DocumentStatus.tsv: downloads/bibo.owl bin/robot.jar
	java -jar bin/robot.jar query --input $< --query sparql/bibo_DocumentStatus.sparql $@
	sed --in-place=.bak 's/^\?//' $@



# NEW BIOSAMPLE INSTANTIATION

api_or_tsv_clean:
	rm -rf assets/out/*
	mkdir -p assets/out
	echo $(PLACEHOLDER_TEXT) > assets/out/README.md


# todo this errors out if 0 valid biosample rows are found
#  do be careful with start and stop values (0-1 is a failure case?)


assets/out/sample_metadata_from_dev_submission_portal.yaml:
	# --data_portal_url https://data.dev.microbiomedata.org/ or https://data.microbiomedata.org/
	$(RUN) biosample_instantiator_plus from-submissions \
		--data_portal_url https://data.dev.microbiomedata.org/ \
		--page_start 0 \
		--page_stop 999 \
		--sample_metadata_csv_file $(basename $@).csv  \
		--sample_metadata_yaml_file $@ \
		--study_metadata_yaml_file $(subst sample,study,$(basename $@)).yaml


# todo switch from csv to tsv or auto-sense

assets/out/sample_metadata_from_dh_csv.yaml:
	$(RUN) biosample_instantiator_plus from-csv \
		--data_csv $(DH_BIOSAMPLE_CSV) \
		--static_dh_template $(DH_TEMPLATE) \
		--static_project_id $(PROJ_ID_FOR_CSV) \
		--sample_metadata_csv_file $(basename $@).csv  \
		--sample_metadata_yaml_file $@

# todo click options should always be hyphen seperated not underscore seperated

assets/out/sample_metadata_from_pure_sqlite.yaml:
	$(RUN) biosample_instantiator_plus pure-from-sqlite \
		--biosample-sql-file $(BIOSAMPLE_SQLITE_FILE) \
		--biosample-id-file $(ACCESSIONS_FILE) \
		--static-project-id $(PROJ_ID_FOR_SQLITE) \
		--sqlite-to-biosample-file assets/in/sqlite_to_v3_plus_v6.tsv \
		--sample-metadata-yaml-file $@

assets/out/sample_metadata_from_pure_gold_api.yaml:
	$(RUN) biosample_instantiator_plus pure-from-gold-study \
			--gold-study-id $(PROJ_ID_FOR_SQLITE) \
			--gold-mapping-file assets/in/gold_nmdc_mapping.tsv \
			--sample-metadata-yaml-file $@

assets/out/sample_metadata_from_hybrid.yaml:
	$(RUN) biosample_instantiator_plus sqlite-gold-hybrid \
			--biosample-id-file $(ACCESSIONS_FILE) \
			--biosample-sql-file $(BIOSAMPLE_SQLITE_FILE) \
			--gold-mapping-file assets/in/gold_nmdc_mapping.tsv \
			--gold-study-id $(PROJ_ID_FOR_SQLITE) \
			--lookup_file assets/in/Angelo_2014-Banfield-JGI-data.tsv \
			--lookup_style mcafes_gold_lookup \
			--sample-metadata-yaml-file $@ \
			--sqlite-to-biosample-file assets/in/sqlite_to_v3_plus_v6.tsv

.PHONY: \
bioscales_dh_csv \
m-CAFEs-hybrid \
m-CAFEs-pure-gold-api \
m-CAFEs-pure-sqlite \
submission-portal

# todo could include initial api_or_tsv_clean before any of these
bioscales_dh_csv:  assets/out/sample_metadata_from_dh_csv_v3.json #  todo complains about post-v3 bioscales slots, and absence of part_of. ignore and consider these samples incompatible with v3?
m-CAFEs-hybrid:  assets/out/sample_metadata_from_hybrid_v3.json
m-CAFEs-pure-gold-api:  assets/out/sample_metadata_from_pure_gold_api_v3.json
m-CAFEs-pure-sqlite:  assets/out/sample_metadata_from_pure_sqlite_v3.json
submission-portal:  assets/out/sample_metadata_from_submission_portal_v3.json # todo complains about post-v3 EMSL etc slots. But there ARE part_of assertions!?  ALSO don't forge that this only queries one of DEV or PROD at this point in time

assets/out/%_v3.json: assets/out/%.yaml
	# todo why is this @type removal required?
	# todo where did it come from?
	cat $(basename $<).json | \
		jq 'del(."@type")'  > \
		$(basename $<)_v6_no_outer_type.json

	$(RUN) add_depth2 \
		--input_json_file $(basename $<)_v6_no_outer_type.json \
		--output_json_file $(basename $<)_v6_no_outer_type_add_depth2.json

	$(RUN) jsonschema -i $(basename $<)_v6_no_outer_type_add_depth2.json assets/in/nmdc_schema_6/nmdc.schema.json

	cat $(basename $<)_v6_no_outer_type_add_depth2.json | \
		jq 'del(.biosample_set[].sample_link)' | \
		jq 'del(.biosample_set[].project_ID)' | \
		$(RUN) jsonschema assets/in/nmdc_schema_3_2/nmdc.schema.json

	cat $(basename $<)_v6_no_outer_type_add_depth2.json | \
		jq 'del(.biosample_set[].sample_link)' | \
		jq 'del(.biosample_set[].project_ID)' > \
		$@

