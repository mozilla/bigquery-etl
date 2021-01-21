CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.telemetry_missing_columns_v1`
AS
SELECT
  TIMESTAMP_TRUNC(submission_timestamp, DAY) AS day,
  'telemetry' AS document_namespace,
  REGEXP_EXTRACT(_TABLE_SUFFIX, r"^(.*)_v.*") AS document_type,
  REGEXP_EXTRACT(_TABLE_SUFFIX, r"^.*_v(.*)$") AS document_version,
  path,
  COUNT(*) AS path_count
FROM
  `moz-fx-data-shared-prod.telemetry_stable.*`,
  UNNEST(
    `moz-fx-data-shared-prod`.udf_js.json_extract_missing_cols(
      additional_properties,
      ["histogram_type"],
      ["activeAddons", "userPrefs", "activeGMPlugins", "simpleMeasurements"]
    )
  ) AS path
WHERE
  date(submission_timestamp) = date_sub(date(current_timestamp), INTERVAL 1 day)
  AND sample_id = 1
GROUP BY
  day,
  document_namespace,
  document_type,
  document_version,
  path
