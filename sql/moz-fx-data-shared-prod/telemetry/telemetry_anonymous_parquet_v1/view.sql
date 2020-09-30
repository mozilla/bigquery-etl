CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_anonymous_parquet_v1` AS
SELECT
  DATE(submission_timestamp) AS submission_date_s3,
  STRUCT( document_id,
    UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,
    metadata.header.date AS `date`,
    IFNULL(metadata.geo.country,
      '??') AS geo_country,
    IFNULL(metadata.geo.city,
      '??') AS geo_city,
    metadata.uri.app_build_id,
    metadata.uri.app_name,
    metadata.uri.app_update_channel,
    normalized_channel ) AS metadata,
  application,
  creation_date,
  id,
  payload,
  type,
  version
FROM
  `moz-fx-data-shared-prod.telemetry.anonymous`
