CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod`.search_derived.mobile_search_clients_last_seen_v1
PARTITION BY
  submission_date
CLUSTER BY
  sample_id,
  client_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  CAST(NULL AS DATE) AS submission_date,
  client_id,
  sample_id,
  app_name,
  normalized_app_name,
  country,
  locale,
  app_version,
  channel,
  os,
  os_version,
  default_search_engine,
  default_search_engine_submission_url,
  distribution_id,
  profile_creation_date,
  profile_age_in_days,
  -- Search data
  organic,
  search_count AS sap,
  unknown,
  tagged_sap,
  tagged_follow_on,
  ad_click,
  search_with_ads,
  CAST(NULL AS INT64) AS total_searches,
  CAST(NULL AS INT64) AS tagged_searches,
  -- Monthly search totals
  [
    STRUCT(CAST(NULL AS STRING) AS key, udf.new_monthly_engine_searches_struct() AS value)
  ] AS engine_searches,
  -- Each of the below is one year of activity, as BYTES.
  CAST(NULL AS BYTES) AS days_seen_bytes,
  CAST(NULL AS BYTES) AS days_searched_bytes,
  CAST(NULL AS BYTES) AS days_tagged_searched_bytes,
  CAST(NULL AS BYTES) AS days_searched_with_ads_bytes,
  CAST(NULL AS BYTES) AS days_clicked_ads_bytes,
  CAST(NULL AS BYTES) AS days_created_profile_bytes
FROM
  `moz-fx-data-shared-prod`.search_derived.mobile_search_clients_daily_v1
WHERE
  FALSE
