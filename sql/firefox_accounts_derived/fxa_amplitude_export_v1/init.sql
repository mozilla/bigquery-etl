CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod`.firefox_accounts_derived.fxa_amplitude_export_v1
PARTITION BY
  (submission_date_pacific)
CLUSTER BY
  (user_id)
AS
WITH columns AS (
  SELECT
    CAST(NULL AS DATE) AS submission_date_pacific,
    CAST(NULL AS STRING) AS user_id,
    CAST(NULL AS STRING) AS insert_id,
    CAST(NULL AS DATETIME) AS timestamp,
    CAST(NULL AS STRING) AS region,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS `language`,
    CAST(NULL AS ARRAY<STRING>) AS services,
    CAST(NULL AS ARRAY<STRING>) AS oauth_client_ids,
    CAST(NULL AS ARRAY<STRING>) AS fxa_services_used,
    CAST(NULL AS ARRAY<STRUCT<key STRING, value INT64>>) AS os_used_month,
    CAST(NULL AS INT64) AS sync_device_count,
    CAST(NULL AS INT64) AS sync_active_devices_day,
    CAST(NULL AS INT64) AS sync_active_devices_week,
    CAST(NULL AS INT64) AS sync_active_devices_month,
    CAST(NULL AS STRING) AS ua_version,
    CAST(NULL AS STRING) AS ua_browser,
    CAST(NULL AS FLOAT64) AS app_version,
    CAST(NULL AS INT64) AS days_seen_bits,
)
SELECT
  *
FROM
  columns
WHERE
  FALSE
