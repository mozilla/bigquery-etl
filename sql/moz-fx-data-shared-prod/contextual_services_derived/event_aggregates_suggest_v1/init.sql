CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.contextual_services_derived.event_aggregates_suggest_v1`
PARTITION BY
  submission_date
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS date) AS submission_date,
  CAST(NULL AS STRING) AS form_factor,
  CAST(NULL AS STRING) AS country,
  CAST(NULL AS STRING) AS advertiser,
  CAST(NULL AS STRING) AS normalized_os,
  CAST(NULL AS STRING) AS release_channel,
  CAST(NULL AS INT64) AS position,
  CAST(NULL AS STRING) AS provider,
  CAST(NULL AS STRING) AS match_type,
  CAST(NULL AS BOOL) AS suggest_data_sharing_enabled,
  CAST(NULL AS INT64) AS impression_count,
  CAST(NULL AS INT64) AS click_count,
  CAST(NULL AS STRING) AS query_type
