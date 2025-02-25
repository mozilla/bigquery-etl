CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.structured_missing_columns`
AS
SELECT
  missing_columns.*,
  existing_schema.table_schema IS NOT NULL AS column_exists_in_schema
FROM
  `moz-fx-data-shared-prod.monitoring_derived.structured_missing_columns_v1` AS missing_columns
LEFT JOIN
  -- Check whether the column actually exists in the schema.
  -- In some cases columns first show up as missing, but are added to the schema after some delay.
  -- In other cases columns show up as missing due to some invalid data being sent that did not
  -- get caught during schema validation in ingestion. For example, sometimes integer values that
  -- are too large for BigQuery cause columns to show up here.
  `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` AS existing_schema
  ON existing_schema.table_schema = CONCAT(missing_columns.document_namespace, "_stable")
  AND existing_schema.table_name = CONCAT(
    missing_columns.document_type,
    "_v",
    missing_columns.document_version
  )
  -- Normalize the column paths and convert them to follow the BigQuery column naming conventions.
  -- The `path` format looks like this: `events`.[...].`timestamp`
  -- The `field_path` format in INFORMATION_SCHEMA.COLUMN_FIELD_PATHS looks like this: events.timestamp
  -- For `labeled_*` metrics, the label is removed to match the column name, e.g.
  -- `metrics.labeled_counter.metric_name.label` is renamed to `metrics.labeled_counter.metric_name`
  -- because the label is in the `key` field and not the name of a column
  AND `moz-fx-data-shared-prod.udf.remove_label_from_metric_path`(
    ARRAY_TO_STRING(
      `moz-fx-data-shared-prod.udf_js.snake_case_columns`(
        REGEXP_EXTRACT_ALL(missing_columns.path, '`(.+?)`')
      ),
      "."
    )
  ) = existing_schema.field_path
