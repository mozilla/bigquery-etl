# glam_etl tests

If you find yourself adding or modifying tests in this directory, I hope you
find the following information user useful.

## Usage

The schemas for each of the queries were populated using the bootstrap script,
which scraped the depedencies for each of the SQL scripts. It is not expected to
be run again. Instead, iterate on a copy of the corresponding `test_minimal`.

In each `test_minimal`, run the `data.py` to generate the necessary assets for
the test. Run the test using the `-k` flag.

For example:

```bash
cd tests/sql/glam-fenix-dev
python org_mozilla_fenix_glam_nightly__extract_user_counts_v1/test_minimal/data.py
pytest -k extract_user_counts
```

## Queries for the source data

The best way to populate data for the tables is to use the source datasets in
the testing or production tables. Here are a few queries to get started.

```sql
SELECT
  *
FROM
  glam_etl.org_mozilla_fenix_glam_nightly__clients_histogram_aggregates_v1
WHERE
  sample_id=1
  AND ARRAY_LENGTH(histogram_aggregates) > 0
LIMIT 1
```

```sql
SELECT
  *
FROM
  glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1
WHERE
  ARRAY_LENGTH(scalar_aggregates) > 0
LIMIT 1
```

```sql
SELECT
  *
FROM
  `glam-fenix-dev.glam_etl.org_mozilla_fenix__clients_daily_scalar_aggregates_metrics_v1`
WHERE
  DATE(_PARTITIONTIME) = "2020-10-01"
LIMIT
  1
```

### Some diagrams

This project is fairly complex. Here's the generated DAG from the dry-runs.

#### Subset of dependencies

![subset](./subset.png)

#### Full overview of dependencies

![full](./full.png)
