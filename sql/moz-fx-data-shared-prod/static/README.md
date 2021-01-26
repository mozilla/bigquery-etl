This directory contains definitions for derived tables in the `static` dataset.

In particular, we have two tables based on a one-time export of Firefox Accounts
extracted from Amplitude and imported to BigQuery.

Tables can be defined as a CSV named `data.csv` and the table will be created by
the `publish_static` script.  An optional `schema.json` and `description.txt`
can be defined in the same directory.  If `schema.json` is not defined, column
names are inferred from the first line of the CSV and are assumed to be strings.
`description.txt` defines the table description in BigQuery.

These should be published to all active projects used for production
ETL and analysis:

```
./script/publish_static --project-id mozdata
./script/publish_static --project-id moz-fx-data-shared-prod
./script/publish_static --project-id moz-fx-data-derived-datasets
```
