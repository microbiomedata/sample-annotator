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

assets/out/biosample_collection.json: $(NMDC_SCHEMA_PATH) clean_loosies
	# mam cc498964-d1da-416d-b353-aecf5f6c749d: only 2 rows but completes and validates
	# 68.4%
	# mam c3870c75-5f0b-47da-a9f3-b4e799c79647 3 rows but 99.8% empty
	# pv 49e40955-31c7-44a7-9e31-8499335019e6 and 822e290d-6837-4956-abb9-996dd5f6d8b9
	# 17 rows 70% empty,
	#   ValueError: Unknown AnalysisTypeEnum enumeration code: metagenomics;metaproteomics
	# d1fd2285-45d6-48d5-be12-391a6a65af84 1 row from rothmanj@uci.edu
	#   water_jgi_mg ? NOT DEFINED YET!
	# 4f188ad8-2731-4635-b401-75e079025f47 MISMATCH and d5f506a2-aa68-4a70-b01a-b5e3e72339d2 VALID soil

	$(RUN) python sample_annotator/clients/nmdc/get_metadata_submissions.py \
		--session_cookie $(SESSION_COOKIE) \
		--study_id "cc498964-d1da-416d-b353-aecf5f6c749d"

#	$(RUN) linkml-validate \
#		--target-class Database \
#		--schema $< $@
