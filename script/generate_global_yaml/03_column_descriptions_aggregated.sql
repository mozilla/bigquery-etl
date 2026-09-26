-- Step 3: Build the final aggregated staging table.
-- Joins all sources with the repeated-columns inventory, applies source priority
-- (glean > bqetl > generated), picks the dominant data_type per column name,
-- and flags columns that already exist in global.yaml.
--
-- Depends on:
--   - moz-fx-dev-cbeck-sandbox.bigquery_metadata.columns_with_dataset_table_counts (column_name, dataset_count, table_count)
--   - moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_table_map (from step 00)
--   - moz-fx-dev-cbeck-sandbox.bigquery_metadata.glean_column_descriptions (from step 01)
--   - moz-fx-dev-cbeck-sandbox.bigquery_metadata.bqetl_column_descriptions (from step 02)
--
-- Run in the sandbox project after steps 00, 01, and 02 are complete.
-- Save results to: moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_descriptions_aggregated
--
-- Usage:
--   bq query --project_id=moz-fx-dev-cbeck-sandbox \
--     --destination_table=moz-fx-dev-cbeck-sandbox:bigquery_metadata.column_descriptions_aggregated \
--     --replace --use_legacy_sql=false < 03_column_descriptions_aggregated.sql
CREATE OR REPLACE TABLE
  `moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_descriptions_aggregated`
AS
WITH
-- Derive data_type info from INFORMATION_SCHEMA (column_table_map + glean descriptions).
-- Pick the most frequent data_type per column_name; break ties alphabetically.
all_types AS (
  SELECT
    column_name,
    data_type
  FROM
    `moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_table_map`
  UNION ALL
  SELECT
    g.column_name,
    type_val AS data_type
  FROM
    `moz-fx-dev-cbeck-sandbox.bigquery_metadata.glean_column_descriptions` g,
    UNNEST(g.data_types) AS type_val
),
dominant_type AS (
  SELECT
    column_name,
    data_type
  FROM
    (
      SELECT
        column_name,
        data_type,
        ROW_NUMBER() OVER (PARTITION BY column_name ORDER BY COUNT(*) DESC, data_type ASC) AS rn
      FROM
        all_types
      GROUP BY
        column_name,
        data_type
    )
  WHERE
    rn = 1
),
-- Flag column names that have more than one distinct data_type.
type_check AS (
  SELECT
    column_name,
    COUNT(DISTINCT data_type) > 1 AS type_conflicts
  FROM
    all_types
  GROUP BY
    column_name
),
-- One row per column_name from the repeated-columns inventory.
-- Note: adjust column names here to match the actual schema of columns_with_dataset_table_counts.
base AS (
  SELECT
    column_name,
    dataset_count,
    table_count,
  FROM
    `moz-fx-dev-cbeck-sandbox.bigquery_metadata.columns_with_dataset_table_counts`
)
SELECT
  b.column_name,
  dt.data_type,
  tc.type_conflicts,
  b.dataset_count,
  b.table_count,
  CASE
    WHEN g.descriptions IS NOT NULL
      THEN 'glean'
    WHEN bq.descriptions IS NOT NULL
      THEN 'bqetl'
    ELSE 'missing'
  END AS source,
  COALESCE(g.descriptions, bq.descriptions) AS descriptions,
FROM
  base b
JOIN
  dominant_type dt
  USING (column_name)
JOIN
  type_check tc
  USING (column_name)
LEFT JOIN
  `moz-fx-dev-cbeck-sandbox.bigquery_metadata.glean_column_descriptions` g
  USING (column_name)
LEFT JOIN
  `moz-fx-dev-cbeck-sandbox.bigquery_metadata.bqetl_column_descriptions` bq
  USING (column_name)
ORDER BY
  b.dataset_count DESC,
  b.column_name ASC
