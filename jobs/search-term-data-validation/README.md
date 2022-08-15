# Search Volume Data Validation

This job contains scripts for evaluating whether our recorded search terms 
(candidate search volume for being sanitized and stored) are changing in ways
that might invalidate assumptions on which we've built our sanitization model.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t search-term-data-validation .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the scripts with: 

```sh   
python data_validation_job.py --data_validation_origin <Name of destination table in either mozdata or shared-prod>
```

The table in mozdata (which we treat as staging) is: `mozdata.search_terms_unsanitized_analysis.prototype_data_validation_metrics`
The table in prod is: `moz-fx-data-shared-prod.search_terms_derived.search_term_data_validation_reports_v1`

## Development

./data_validation_job.py is the main control script
./data_validation.py is the module containing the python code the script calls
/tests contains unit tests for functions in ./data_validation.py

