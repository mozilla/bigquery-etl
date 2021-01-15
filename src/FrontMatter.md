_Created 2020-06-18. Updated 2021-01-14_

This network represents the relationships between tables in BigQuery. Each blue
node represents a table, while each orange node represents a dataset. The
network was created by scraping the BigQuery `TABLES` and `JOBS_BY_PROJECT`
tables in the `INFORMATION_SCHEMA` dataset. Views are resolved using bq with
`--dry_run`.

The source can be found at
[acmiyaguchi/etl-graph](https://github.com/acmiyaguchi/etl-graph). See
[NOTES.md](https://github.com/acmiyaguchi/etl-graph/blob/main/README.md) for an
overview of development. This visualizaton is powered by
[vis-network](https://visjs.github.io/vis-network/docs/network/).
