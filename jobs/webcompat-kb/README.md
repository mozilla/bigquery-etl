# Web compatibility Knowledge Base bugs import from bugzilla 

This job fetches bugzilla bugs from Web Compatibility > Knowledge Base component, 
as well as their core bugs dependencies and breakage reports and puts them into BQ.  

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t webcompat-kb .
```

To run locally, first install dependencies in `jobs/webcompat-kb`:

```sh
pip install -r requirements.txt
```

And then run the script after authentication with gcloud: 

```sh
gcloud auth application-default login
python3 webcompat_kb/main.py --bq_project_id=<your_project_id> --bq_dataset_id=<your_dataset_id>
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
flake8 webcompat_kb/ tests/
black --diff webcompat_kb/ tests/
```
