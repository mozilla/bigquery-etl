-- Query for monitoring_derived.metadata_standardization_v1
WITH all_columns AS (
  SELECT
    table_catalog AS project_id,
    table_schema AS dataset_id,
    field_path,
    SPLIT(field_path, '.') AS path_parts,
    SPLIT(field_path, '.')[ORDINAL(ARRAY_LENGTH(SPLIT(field_path, '.')))] AS column_name,
    description
  FROM
    `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
),
-- Detect parent paths to identify non-leaf columns, those of type RECORD with nested columns.
non_leaf_prefixes AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(field_path, r'^(.*)\.[^\.]+$') AS parent_path
  FROM
    all_columns
  WHERE
    field_path LIKE '%.%'
),
-- Keep only leaf-level fields, those with no children.
leaf_columns AS (
  SELECT
    *
  FROM
    all_columns
  WHERE
    field_path NOT IN (SELECT parent_path FROM non_leaf_prefixes)
),
-- Normalize descriptions, setting NULL as "NULL".
column_group AS (
  SELECT
    project_id,
    dataset_id,
    column_name,
    ARRAY_AGG(DISTINCT IF(description IS NULL, 'NULL', description)) AS descriptions
  FROM
    leaf_columns
  GROUP BY
    project_id,
    dataset_id,
    column_name
),
-- Roll up per dataset.
dataset_summary AS (
  SELECT
    project_id,
    dataset_id,
    COUNT(*) AS total_columns,
    COUNTIF(
      EXISTS(SELECT 1 FROM UNNEST(descriptions) d WHERE d != 'NULL' AND TRIM(d) != '')
    ) AS columns_with_non_null_description,
    COUNTIF(ARRAY_LENGTH(descriptions) > 1) AS columns_with_multiple_descriptions,
    ARRAY_AGG(STRUCT(column_name, descriptions)) AS column_description_map
  FROM
    column_group
  GROUP BY
    project_id,
    dataset_id
)
SELECT
  @submission_date AS submission_date,
  project_id,
  dataset_id,
  total_columns,
  columns_with_non_null_description,
  columns_with_multiple_descriptions,
  ROUND(100 * (columns_with_non_null_description / total_columns), 2) AS completeness_percentage,
  ROUND(
    100 * (1 - (dataset_summary.columns_with_multiple_descriptions / total_columns)),
    2
  ) AS standardization_percentage,
  column_description_map
FROM
  dataset_summary
