# Quicksuggest Remote Settings import

This dockerized Python job reads and parses Quicksuggest (Firefox Suggest)
suggestions from Remote Settings (Kinto server) and appends them to the
referenced BigQuery table.

## Usage

Import data to BigQuery by running `python3 quicksuggest_2_bq/main.py`:

```
Usage: main.py [OPTIONS]

Options:
  --date [%Y-%m-%d]            date for which to store the results  [required]
  --destination-project TEXT   the GCP project to use for writing data to
                               [required]

  --destination-table-id TEXT  the table id to append data to, e.g.
                               `projectid.dataset.table`  [required]

  --kinto-server TEXT          the Kinto server to fetch the data from
  --kinto-bucket TEXT          the Kinto bucket to fetch the data from
  --kinto-collection TEXT      the Kinto server to fetch the data from
  --help                       Show this message and exit.
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
flake8 quicksuggest_2_bq/ tests/
black --diff quicksuggest_2_bq/ tests/
```
