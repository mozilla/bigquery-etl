# Experiments Monitoring Data Export

This script exports views related to experiment monitoring to GCS as JSON. The script are scheduled
via Airflow and are executed every 5 minutes to ensure exported data stays up-to-date.

The exported monitoring data is displayed in the Experimenter console and allows experiment owners
to monitor how their deployed experiments are performing.

## Usage

Export data by running `python3 experiments_monitoring_data_export/export.py`:
```
usage: export.py [-h] [--source_project SOURCE_PROJECT] [--destination_project DESTINATION_PROJECT] [--bucket BUCKET] [--gcs_path GCS_PATH] [--datasets [DATASETS [DATASETS ...]]]

Exports experiment monitoring data to GCS as JSON.

optional arguments:
  -h, --help            show this help message and exit
  --source_project SOURCE_PROJECT, --source-project SOURCE_PROJECT
                        Project containing views to be exported
  --destination_project DESTINATION_PROJECT, --destination-project DESTINATION_PROJECT
                        Project with bucket data is exported to
  --bucket BUCKET       GCS bucket data is exported to
  --gcs_path GCS_PATH, --gcs-path GCS_PATH
                        GCS path data is written to
  --datasets [DATASETS [DATASETS ...]]
                        Experiment monitoring datasets to be exported
```
