RUN = poetry run

.PHONY: test clean all

all: clean test examples/report.tsv

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
	$(RUN) sa_sa -R $@ $<
