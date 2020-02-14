CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_downgrade_parquet_v1` AS
SELECT
  DATE(submission_timestamp) AS submission_date,
  `id`,
  creation_date,
  application,
  client_id,
  version,
  STRUCT(payload.last_version,
    payload.last_build_id,
    payload.has_sync,
    CAST(NULL AS boolean) AS has_binary,
    CAST(payload.button AS int64) AS button ) AS payload
FROM
  `moz-fx-data-shared-prod.telemetry.downgrade`
