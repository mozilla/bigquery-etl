CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_mobile_event_parquet_v2` AS
SELECT
  DATE(submission_timestamp) AS submission_date_s3,
  DATE(submission_timestamp) AS submission_date,
  metadata.uri.app_name,
  os,
  STRUCT( document_id,
    UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,
    metadata.header.date AS `date`,
    IFNULL(metadata.geo.country,
      '??') AS geo_country,
    IFNULL(metadata.geo.city,
      '??') AS geo_city,
    metadata.uri.app_build_id,
    metadata.uri.app_name,
    metadata.uri.app_version,
    sample_id,
    metadata.uri.app_update_channel ) AS metadata,
  v,
  client_id,
  seq,
  locale,
  osversion,
  device,
  arch,
  created,
  process_start_timestamp,
  tz,
  settings,
  ARRAY(
    SELECT
      AS STRUCT f0_ AS timestamp,
      f1_ AS category,
      f2_ AS method,
      f3_ AS object,
      f4_ AS value,
      f5_ AS extra
  FROM
    UNNEST(events)) AS events,
  experiments
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_event`
