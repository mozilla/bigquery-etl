CREATE OR REPLACE TABLE
  org_mozilla_firefox_derived.events_daily_v1
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS date) AS submission_date,
  CAST(NULL AS STRING) AS client_id,
  CAST(NULL AS INT64) AS sample_id,
  CAST(NULL AS STRING) AS events,
  -- client info
  CAST(NULL AS STRING) AS android_sdk_version,
  CAST(NULL AS STRING) AS app_build,
  CAST(NULL AS STRING) AS app_channel,
  CAST(NULL AS STRING) AS app_display_version,
  CAST(NULL AS STRING) AS architecture,
  CAST(NULL AS STRING) AS device_manufacturer,
  CAST(NULL AS STRING) AS device_model,
  CAST(NULL AS STRING) AS first_run_date,
  CAST(NULL AS STRING) AS telemetry_sdk_build,
  CAST(NULL AS STRING) AS locale,
  -- metadata
  CAST(NULL AS STRING) AS city,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS STRING) AS subdivision1,
  -- normalized fields
  CAST(NULL AS STRING) AS channel,
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS os_version,
  -- ping info
  CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS experiments
FROM
  org_mozilla_firefox.events
WHERE
  FALSE
