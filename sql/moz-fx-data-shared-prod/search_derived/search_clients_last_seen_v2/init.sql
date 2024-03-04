CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_derived.search_clients_last_seen_v2`
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
  scd.client_id,
  scd.sample_id,
    -- Dimensional data
  scd.country,
  scd.app_version,
  scd.distribution_id,
  scd.locale,
  scd.search_cohort,
  scd.addon_version,
  scd.os,
  scd.channel,
  cfs.first_seen_date,
  scd.default_search_engine,
  scd.default_search_engine_data_load_path,
  scd.default_search_engine_data_submission_url,
  CAST(NULL AS INT64) AS days_since_first_seen,
  scd.active_addons_count_mean,
  scd.user_pref_browser_search_region,
  scd.os_version,
  cfs.country AS first_reported_country,
    -- User activity data
  scd.max_concurrent_tab_count_max,
  scd.tab_open_event_count_sum,
  scd.active_hours_sum,
  scd.subsession_hours_sum,
  scd.sessions_started_on_this_day,
  scd.total_uri_count AS total_uri_count_sum,
    -- Search data
  scd.organic,
  scd.sap,
  scd.unknown,
  scd.tagged_sap,
  scd.tagged_follow_on,
  scd.ad_click,
  scd.search_with_ads,
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
  CAST(NULL AS BYTES) AS days_first_seen_bytes,
  CAST(NULL AS BYTES) AS days_dau_bytes,
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8` scd
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.clients_first_seen` cfs
  USING (client_id)
WHERE
  FALSE
