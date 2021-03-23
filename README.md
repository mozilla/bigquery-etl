# Generated SQL

This `generated-sql` branch is based on the `sql` directory of the 
[default branch](https://github.com/mozilla/bigquery-etl),
but includes additional programmatically generated content, including:

- Views on top of stable ping tables
- Derived tables giving daily aggregations on top of Glean `baseline` pings

Currently, this branch is used only as the basis for generating 
[documentation on user-facing views](https://mozilla.github.io/bigquery-etl/mozdata/introduction/),
but it may evolve into an artifact used for deployment and management of
derived tables in BigQuery.

See the
`generate-sql` task in the repository's 
[.circleci/config.yml](https://github.com/mozilla/bigquery-etl/blob/master/.circleci/config.yml)
for the specifics of what gets generated.
