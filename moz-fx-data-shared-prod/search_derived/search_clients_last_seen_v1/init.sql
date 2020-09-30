CREATE TABLE
  search_clients_last_seen_v1
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
    -- Grouping columns
  client_id,
  sample_id,
    -- Dimensional data
  country,
  app_version,
  distribution_id,
  locale,
  search_cohort,
  addon_version,
  os,
  channel,
  profile_creation_date,
  default_search_engine,
  default_search_engine_data_load_path,
  default_search_engine_data_submission_url,
  profile_age_in_days,
  active_addons_count_mean,
  user_pref_browser_search_region,
  os_version,
    -- User activity data
  max_concurrent_tab_count_max,
  tab_open_event_count_sum,
  active_hours_sum,
  subsession_hours_sum,
  sessions_started_on_this_day,
    -- Search data
  organic,
  sap,
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
  search_clients_daily_v8
WHERE
  FALSE
