CREATE OR REPLACE TABLE
  {{ app_id }}_derived.events_daily_v1
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
  {% for property in user_properties %}
    CAST(NULL AS STRING) AS {{ property.dest }},
  {% endfor %}
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
