# Makefile to format and validate scripts inside the bigquery-etl repository.

help:
	@echo "Scripts validation and formatting\n"
	@echo "The list of available commands:\n"
	@echo "  format     Runs a set of steps to correctly format the files in the specified path."
	@echo "  validate   Runs a validation for the correct format for all files in the specified path."

format:
	@echo "Applying format to all queries..."
	./bqetl query format $(git ls-tree -d HEAD --name-only)

validate:
	@echo "Running Yamllint test..."
	venv/bin/yamllint -c .yamllint.yaml .

	@echo "Validating metadata..."
	script/validate_metadata

	@echo "Validating scripts format..."
	script/bqetl format --check $(git ls-tree -d HEAD --name-only)

	@echo "Running PyTest with linters..."
	script/entrypoint --black --flake8 \
        --mypy-ignore-missing-imports --pydocstyle \
        -m "not (routine or sql or integration or java)" \
        -n 8

	@echo "Runninbg SQL tests. Requires gcloud authentication!..."
	@echo "script/entrypoint -m "routine or sql" -n 8"

	@echo "Dry run the queries..."
	script/dryrun --validate-schemas sql/*

	@echo "Validating docs..."
	script/validate_docs

	@echo "Validations complete"