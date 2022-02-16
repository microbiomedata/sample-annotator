RUN = poetry run

.PHONY: test clean all

all: clean test examples/outputs/report.tsv

# ---------------------------------------
# Test runner
# ----------------------------------------
test:
	$(RUN) pytest 2>&1 | tee logs/tests.log
	$(RUN) pytest -sv tests/test_capitalization.py


clean:
	find examples -name "*report.tsv" -exec rm -rf {} \;
	rm -rf logs/*log
	rm -rf examples/outputs/*


examples/outputs/report.tsv: examples/gold.json
	$(RUN) annotate-sample -R $@ $<

biosample_sqlite_file = ~/biosample_basex_data_good_subset.db

# todo: isolate client from application code
biosample_sqlite_poetry_script:
	$(RUN) sqlite_client_cli --sqlite_path $(biosample_sqlite_file)

