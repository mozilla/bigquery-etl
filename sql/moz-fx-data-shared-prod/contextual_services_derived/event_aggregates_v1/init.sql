CREATE OR REPLACE TABLE
  contextual_services_derived.event_aggregates_v1
PARTITION BY
  submission_date
CLUSTER BY
  source,
  event_type
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS date) AS submission_date,
  CAST(NULL AS STRING) AS source,
  CAST(NULL AS STRING) AS event_type,
  CAST(NULL AS STRING) AS form_factor,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS STRING) AS subdivision1,
  CAST(NULL AS STRING) AS advertiser,
  CAST(NULL AS STRING) AS release_channel,
  CAST(NULL AS INT64) AS position,
  CAST(NULL AS INT64) AS event_count,
  CAST(NULL AS INT64) AS user_count,
  CAST(NULL AS STRING) AS match_type,
  CAST(NULL AS STRING) AS normalized_os,
  CAST(NULL AS BOOL) AS suggest_data_sharing_enabled,
  CAST(NULL AS STRING) AS query_type,
