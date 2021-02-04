CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_focus_event_parquet_v1` AS
SELECT
  DATE(submission_timestamp) AS submission_date_s3,
  DATE(submission_timestamp) AS submission_date,
  metadata.uri.app_update_channel AS channel,
  STRUCT( document_id,
    UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,
    metadata.header.date AS `date`,
    IFNULL(metadata.geo.country,
      '??') AS geo_country,
    IFNULL(metadata.geo.city,
      '??') AS geo_city,
    metadata.uri.app_build_id,
    metadata.uri.app_name ) AS metadata,
  v,
  client_id,
  seq,
  locale,
  os,
  osversion,
  created,
  tz,
  settings,
  events,
  arch,
  device,
  process_start_timestamp,
  experiments
FROM
  `moz-fx-data-shared-prod.telemetry.focus_event`
