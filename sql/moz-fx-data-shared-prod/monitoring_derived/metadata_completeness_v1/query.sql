--Get all objects in moz-fx-data-shared-prod or mozdata
WITH all_mozdata_and_shared_prod_objects AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
  FROM
    `moz-fx-data-shared-prod.region-US.INFORMATION_SCHEMA.TABLES`
  UNION ALL
  SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
  FROM
    `mozdata.region-US.INFORMATION_SCHEMA.TABLES`
  WHERE
    LOWER(table_schema) <> 'looker_tmp'
),
--get the object descriptions if there are any
mozdata_and_shared_prod_object_descriptions AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    option_value AS object_description
  FROM
    `moz-fx-data-shared-prod.region-US.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = 'description'
  UNION ALL
  SELECT
    table_catalog,
    table_schema,
    table_name,
    option_value AS object_description
  FROM
    `mozdata.region-US.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = 'description'
),
--get the # of columns on each object and how many have a description
shared_prod_and_mozdata_column_metadata_completeness AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    COUNT(1) AS nbr_columns,
    SUM(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) AS nbr_columns_with_non_null_desc
  FROM
    `mozdata.region-US.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
  GROUP BY
    table_catalog,
    table_schema,
    table_name
  UNION ALL
  SELECT
    table_catalog,
    table_schema,
    table_name,
    COUNT(1) AS nbr_columns,
    SUM(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) AS nbr_columns_with_non_null_desc
  FROM
    `moz-fx-data-shared-prod.region-US.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
  GROUP BY
    table_catalog,
    table_schema,
    table_name
)
SELECT
  @submission_date AS submission_date,
  all_obj.table_catalog,
  all_obj.table_schema,
  all_obj.table_name,
  all_obj.table_type,
  obj_desc.object_description,
  col_level_completeness.nbr_columns,
  col_level_completeness.nbr_columns_with_non_null_desc
FROM
  all_mozdata_and_shared_prod_objects all_obj
LEFT OUTER JOIN
  mozdata_and_shared_prod_object_descriptions obj_desc
  ON all_obj.table_catalog = obj_desc.table_catalog
  AND all_obj.table_schema = obj_desc.table_schema
  AND all_obj.table_name = obj_desc.table_name
LEFT OUTER JOIN
  shared_prod_and_mozdata_column_metadata_completeness col_level_completeness
  ON all_obj.table_catalog = col_level_completeness.table_catalog
  AND all_obj.table_schema = col_level_completeness.table_schema
  AND all_obj.table_name = col_level_completeness.table_name
