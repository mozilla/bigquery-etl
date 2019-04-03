CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_v1`
AS
WITH
  clients_daily_with_usage_array AS (
  SELECT
    *,
    ['Any Firefox Desktop Activity',
    IF(devtools_toolbox_opened_count_sum > 0,
      'Firefox Desktop Dev Tools Opened',
      NULL) ] AS usage_ids
  FROM
    `moz-fx-data-derived-datasets.telemetry.clients_daily_v6` )
SELECT
  submission_date_s3 AS date,
  client_id AS profile_id,
  usage_id,
  country
FROM
  clients_daily_with_usage_array,
  UNNEST(usage_ids) AS usage_id
WHERE
  usage_id IS NOT NULL
