CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.aggregated_search_terms_daily_v1`
PARTITION BY
  submission_date
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS DATE) AS submission_date,
  CAST(NULL AS STRING) AS search_terms,
  CAST(NULL AS INT64) AS impressions,
  CAST(NULL AS INT64) AS clicks,
  CAST(NULL AS INT64) AS client_days,
