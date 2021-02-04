CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_heartbeat_parquet_v1` AS
SELECT
  DATE(submission_timestamp) AS submission_date_s3,
  DATE(submission_timestamp) AS submission_date,
  type,
  id,
  creation_date,
  version,
  client_id,
  application,
  (SELECT AS STRUCT payload.* replace (CAST(version AS int64) AS version)) AS payload,
  STRUCT( UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,
    metadata.header.date AS `date`,
    IFNULL(metadata.geo.country,
      '??') AS geo_country,
    IFNULL(metadata.geo.city,
      '??') AS geo_city,
    document_id,
    metadata.uri.app_build_id,
    metadata.uri.app_name ) AS metadata,
  engagement_type
FROM
  `moz-fx-data-shared-prod.telemetry.heartbeat`
