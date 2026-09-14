-- Step 0: Build a map of (column_name, dataset_id, table_name) for all non-_stable/non-_live
-- tables in moz-fx-data-shared-prod. This is used by the Python script to locate query.sql
-- files in the bqetl repo for columns that have no existing description.
--
-- Run this query in the moz-fx-data-shared-prod project (region: us).
-- Save results to: moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_table_map
--
-- Usage:
--   bq query --project_id=moz-fx-data-shared-prod \
--     --destination_table=moz-fx-dev-cbeck-sandbox:bigquery_metadata.column_table_map \
--     --replace --use_legacy_sql=false < 00_column_table_map.sql
CREATE OR REPLACE TABLE
  `moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_table_map`
AS
SELECT
  REGEXP_EXTRACT(field_path, r'([^.]+)$') AS column_name,
  table_schema AS dataset_id,
  table_name,
  data_type,
FROM
  `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE
  table_schema NOT LIKE '%_stable'
  AND table_schema NOT LIKE '%_live'
ORDER BY
  column_name,
  dataset_id,
  table_name
