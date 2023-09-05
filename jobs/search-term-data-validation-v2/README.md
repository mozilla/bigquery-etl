# Search Volume Data Validation

This job contains scripts for evaluating whether our recorded search terms 
(candidate search volume for being sanitized and stored) are changing in ways
that might invalidate assumptions on which we've built our sanitization model.

**Why 'v2' in the name?**
The original one broke after a few refactors to the repo's template code.
Four separate engineers over two months did not successfully identify the problem.
So we went with recreating from scratch, which also allowed us to exercise the template changes.
Bumping the version on the original one wouldn't have acomplished this.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t search-term-data-validation-v2 .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the scripts with: 

```sh   
python search_term_data_validation_v2/main.py --data_validation_origin <table_name> --data_validation_reporting_destination <table_name>
```

The origin table in mozdata (which we treat as staging) is: `mozdata.search_terms_unsanitized_analysis.prototype_data_validation_metrics`
The origin table in prod is: `moz-fx-data-shared-prod.search_terms.sanitization_job_data_validation_metrics`

The destination table in mozdata (which we treat as staging) is: `mozdata.search_terms_unsanitized_analysis.prototype_data_validation_reports_v1`
The destination table in prod is: `moz-fx-data-shared-prod.search_terms_derived.search_term_data_validation_reports_v1`

## Development

`python search_term_data_validation_v2/main.py` is the main control script
`python search_term_data_validation_v2/data_validation.py` is the module containing the python code the script calls
`tests` contains unit tests for functions in `python search_term_data_validation_v2/data_validation.py`

