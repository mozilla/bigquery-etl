# Generated SQL

This `generated-sql` branch is based on the `sql` directory of the 
[default branch](https://github.com/mozilla/bigquery-etl),
but includes additional programmatically generated content, including:

- Views on top of stable ping tables
- Derived tables giving daily aggregations on top of Glean `baseline` pings
- `metadata.yaml` file that includes more information about the views, including referenced tables

Currently, this branch is used as the basis for generating 
[documentation on user-facing views](https://mozilla.github.io/bigquery-etl/mozdata/introduction/),
and for retrieving table references during [LookML generation](https://github.com/mozilla/lookml-generator/blob/main/generator/namespaces.py#L37).
It may evolve into an artifact used for deployment and management of
derived tables in BigQuery.

See the
`generate-sql` task in the repository's 
[.circleci/config.yml](https://github.com/mozilla/bigquery-etl/blob/master/.circleci/config.yml)
for the specifics of what gets generated.
