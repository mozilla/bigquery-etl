# glam_etl tests

If you find yourself adding or modifying tests in this directory, I hope you find the following commands somewhat useful.

```bash
gcloud config set project glam-fenix-dev
```

Get a row from a table:

```bash
bq query --use_legacy_sql=false --format json 'select * from glam_etl.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1 where sample_id=1 limit 1'
```