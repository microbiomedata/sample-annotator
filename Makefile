#
#all: $(SAMPLE_SCHEMA_JSON)
#RUN = pipenv run
#SAMPLE_SCHEMA_YAML = sample_annotator/model/schema/mixs.yaml
#SAMPLE_SCHEMA_JSON = sample_annotator/model/schema/mixs.json
#
#$(SAMPLE_SCHEMA_YAML):
#	$(RUN) gen-yaml --mergeimports --no-metadata ../mixs-source/src/schema/mixs.yaml > $@.tmp && mv $@.tmp $@
#
#$(SAMPLE_SCHEMA_JSON): $(SAMPLE_SCHEMA_YAML)
#	 ./utils/yaml2json.py $< > $@
#
#.PHONY: requirements-file
#requirement-files: requirements.txt requirements-dev.txt
## calls pipenv to generate the requirements.txt and requirements-dev.txt files
#	pipenv run pipenv_to_requirements
#
#Pipfile.lock: Pipfile
## generate Pipfile.lock if Pipfile changes
#	pipenv install --dev

.PHONY: test clean all

# ---------------------------------------
# Test runner
# ----------------------------------------
test:
	poetry run python -m unittest
