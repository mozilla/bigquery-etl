# InfluxDB to Bigquery import job

This job collects AMP data from Influxdb and puts them into BQ.

For more information on see https://mozilla-hub.atlassian.net/browse/RS-683

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
podman build -t influxdb-to-bigquery .
```

To run locally, install dependencies with (in jobs/influxdb-to-bigquery):

```sh
cd ..
pip install -r requirements.txt
```

Run the script with (needs gcloud auth):

```sh
python3 influxdb_to_bigquery/main.py "--bq_project_id"=test_bq_project "--bq_dataset_id"=test_bq_dataset "--bq_table_id"=test_bq_table "--influxdb_measurement"=test_influx_measurement "--influxdb_username"=test_influx_un "--influxdb_password"=test_influx_pwd "--influxdb_host"=test_influx_host --date=test_date
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
flake8 python_template_job/ tests/
black --diff python_template_job/ tests/
```
