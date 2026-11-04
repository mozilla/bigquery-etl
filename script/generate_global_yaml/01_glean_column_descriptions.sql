-- Step 1: Collect column descriptions from all _stable tables (Glean telemetry source).
-- These descriptions come from Glean metric definitions and are the most authoritative source.
--
-- Run this query in the moz-fx-data-shared-prod project (region: us).
-- Save results to: moz-fx-dev-cbeck-sandbox.bigquery_metadata.glean_column_descriptions
--
-- Usage:
--   bq query --project_id=moz-fx-data-shared-prod \
--     --destination_table=moz-fx-dev-cbeck-sandbox:bigquery_metadata.glean_column_descriptions \
--     --replace --use_legacy_sql=false < 01_glean_column_descriptions.sql
CREATE OR REPLACE TABLE
  `moz-fx-dev-cbeck-sandbox.bigquery_metadata.glean_column_descriptions`
AS
SELECT
  REGEXP_EXTRACT(field_path, r'([^.]+)$') AS column_name,
  ARRAY_AGG(DISTINCT data_type IGNORE NULLS ORDER BY data_type) AS data_types,
  ARRAY_AGG(DISTINCT description IGNORE NULLS ORDER BY description) AS descriptions,
  COUNT(DISTINCT table_name) AS table_count
FROM
  `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE
  table_schema LIKE '%_stable'
  AND table_schema != 'analysis'
  AND description IS NOT NULL
  AND TRIM(description) != ''
  -- Exclude STRUCT/ARRAY container fields — only leaf fields have descriptions
  AND data_type NOT LIKE 'STRUCT<%'
  AND data_type NOT LIKE 'ARRAY<%'
GROUP BY
  column_name
ORDER BY
  table_count DESC
