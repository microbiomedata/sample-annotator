RUN = poetry run

#biosample_sqlite_file = ~/biosample_basex_data_good_subset.db
# curling from NERSC portal now
# but may want to change local destination... like a data directory?
# NOTE: this database file will be deleted by make clean. Don't do any manual modifications in there!
biosample_sqlite_file = biosample_basex_data_good_subset.db

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
	rm -rf $(biosample_sqlite_file)

examples/outputs/report.tsv: examples/gold.json
	$(RUN) annotate-sample -R $@ $<

downloads/mixs6_core.tsv:
	curl -L -s 'https://docs.google.com/spreadsheets/d/1QDeeUcDqXes69Y2RjU2aWgOpCVWo5OVsBX9MKmMqi_o/export?format=tsv&gid=178015749' > $@

examples/outputs/non_attribute_metadata_sel_envs_partial.tsv: $(biosample_sqlite_file)
	$(RUN) sqlite_client_cli \
		--sqlite_path $(biosample_sqlite_file) \
		--query "select * from non_attribute_metadata_sel_envs limit 9" \
		--tsv_out $@

rel_to_oxygen_example: downloads/mixs6_core.tsv $(biosample_sqlite_file)
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

# full db at https://portal.nersc.gov/project/m3513/biosample/biosample_basex.db.gz
# subset has few tables
# fewer rows in XXX (corresponding to samples with reapired env package values of XXX...)
#   see XXX
# and fewer columns in XXX, highlighting
downloads/biosample_basex_data_good_subset.db.zip:
	# --location (-L) pursues redirects
	curl --location https://portal.nersc.gov/project/m3513/biosample/biosample_basex_data_good_subset.db.zip -o $@

# unzipped file goes into the cwd by default, which would usually be the root of the project
biosample_basex_data_good_subset.db: downloads/biosample_basex_data_good_subset.db.zip
	unzip $<
