# Python Template Job

This is an example of a dockerized Python job.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t broken-site-report-ml .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

And then run the script after authentication with gcloud:

```sh   
gcloud auth application-default login
python3 broken_site_report_ml/main.py --bq_project_id=<your_project_id> --bq_dataset_id=<your_dataset_id>
```

## Development

Run tests with:

```sh
pytest
```

`flake8` and `black` are included for code linting and formatting:

```sh
pytest --black --flake8
```

or

```sh
flake8 broken_site_report/ tests/
black --diff broken_site_report/ tests/
```
