# Play Store Export

#### Imported from https://github.com/mozilla/play-store-export

This Play Store export is a job to schedule backfills of Play Store data to BigQuery via the BigQuery Data Transfer service.

The purpose of this job is to be scheduled to run regularly in order to continuously backfill past days over time.
Past Play Store data has been found to still update over time (e.g. data from a day two weeks ago can still be updated)
so regular backfills of at least 30 days are required.
This is an issue with the retained installers metric in particular.
The BigQuery Play Store transfer job has a non-configurable refresh window size of 7 days which is insufficient.

These scripts require that a Play Store transfer config already exists and the current gcloud user has
permission to create jobs in the project.

See [Google Play transfers documentation](https://cloud.google.com/bigquery-transfer/docs/play-transfer) for more details.

## Usage

Start a backfill using the `python3 play_store_export/export.py` script:
```sh
usage: export.py [-h] --date DATE --project PROJECT --transfer-config
                 TRANSFER_CONFIG [--transfer-location TRANSFER_LOCATION]
                 [--backfill-day-count BACKFILL_DAY_COUNT]

optional arguments:
  -h, --help            show this help message and exit
  --date DATE           Date at which the backfill will start, going backwards
  --project PROJECT     Either the project that the source GCS project belongs
                        to or the project that contains the transfer config
  --transfer-config TRANSFER_CONFIG
                        ID of the transfer config. This should be a UUID.
  --transfer-location TRANSFER_LOCATION
                        Region of the transfer config (defaults to `us`)
  --backfill-day-count BACKFILL_DAY_COUNT
                        Number of days to backfill
```

## Develop

This project uses the BigQuery Data Transfer Python library: 
https://googleapis.dev/python/bigquerydatatransfer/latest/index.html

Install python dependencies with:
```sh
make install
```

Run tests with:
```sh
make test
```

Run linter with:
```sh
make lint
```

A script to cancel all running transfer jobs exists in `play_store_export/cancel_transfers.py`
which may be useful during development and testing.

