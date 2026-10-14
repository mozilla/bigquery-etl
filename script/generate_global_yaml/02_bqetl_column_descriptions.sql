-- Step 2: Collect column descriptions from bqetl-derived tables in BigQuery.
-- Excludes _stable and _live tables (those are covered by Step 1).
-- Queries BigQuery directly to capture tables that may lack schema.yaml files
-- or have incomplete descriptions there.
--
-- Run this query in the moz-fx-data-shared-prod project (region: us).
-- Save results to: moz-fx-dev-cbeck-sandbox.bigquery_metadata.bqetl_column_descriptions
--
-- Usage:
--   bq query --project_id=moz-fx-data-shared-prod \
--     --destination_table=moz-fx-dev-cbeck-sandbox:bigquery_metadata.bqetl_column_descriptions \
--     --replace --use_legacy_sql=false < 02_bqetl_column_descriptions.sql
CREATE OR REPLACE TABLE
  `moz-fx-dev-cbeck-sandbox.bigquery_metadata.bqetl_column_descriptions`
AS
SELECT
  REGEXP_EXTRACT(field_path, r'([^.]+)$') AS column_name,
  ARRAY_AGG(DISTINCT data_type IGNORE NULLS ORDER BY data_type) AS data_types,
  ARRAY_AGG(DISTINCT description IGNORE NULLS ORDER BY description) AS descriptions,
  COUNT(DISTINCT table_name) AS table_count
FROM
  `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE
  table_schema NOT LIKE '%_stable'
  AND table_schema NOT LIKE '%_live'
  AND table_schema != 'analysis'
  AND description IS NOT NULL
  AND TRIM(description) != ''
  -- Exclude STRUCT/ARRAY container fields — only leaf fields have meaningful descriptions
  AND data_type NOT LIKE 'STRUCT<%'
  AND data_type NOT LIKE 'ARRAY<%'
GROUP BY
  column_name
ORDER BY
  table_count DESC
