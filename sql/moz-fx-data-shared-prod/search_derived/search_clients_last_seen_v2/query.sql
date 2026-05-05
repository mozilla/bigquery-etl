WITH _derived_search_cols AS (
  SELECT
    COALESCE(
      `moz-fx-data-shared-prod.udf.normalize_search_engine`(engine),
      "Other"
    ) AS short_engine,
    COALESCE(organic, 0) + COALESCE(sap, 0) + COALESCE(unknown, 0) + COALESCE(
      tagged_sap,
      0
    ) + COALESCE(tagged_follow_on, 0) AS total_searches,
    COALESCE(tagged_sap, 0) + COALESCE(tagged_follow_on, 0) AS tagged_searches,
    COALESCE(ad_click, 0) AS ad_click,
    COALESCE(search_with_ads, 0) AS search_with_ads,
    scd.* EXCEPT (ad_click, search_with_ads),
    first_seen_date,
    cfs.country AS first_reported_country,
    DATE_DIFF(@submission_date, first_seen_date, DAY) AS days_since_first_seen
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8` scd
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry.clients_first_seen` cfs
    USING (client_id)
  WHERE
    scd.submission_date = @submission_date
),
_derived_engine_searches AS (
    -- From the clients search info, make a struct
    -- that we will use for aggregation later
  SELECT
    STRUCT(
      short_engine AS key,
      STRUCT(total_searches, tagged_searches, ad_click, search_with_ads) AS value
    ) AS engine_searches,
    * EXCEPT (short_engine)
  FROM
    _derived_search_cols
),
_grouped AS (
    -- Here we get a single row per-client, containing
    -- info from each engine, as well as overall info
  SELECT
      -- Grouping columns
    client_id,
    sample_id,
      -- Dimensional data
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(country)) AS country,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(app_version)) AS app_version,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(distribution_id)) AS distribution_id,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(locale)) AS locale,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(search_cohort)) AS search_cohort,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(addon_version)) AS addon_version,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(os)) AS os,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(channel)) AS channel,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(first_seen_date)) AS first_seen_date,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(default_search_engine)
    ) AS default_search_engine,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(default_search_engine_data_load_path)
    ) AS default_search_engine_data_load_path,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(default_search_engine_data_submission_url)
    ) AS default_search_engine_data_submission_url,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(days_since_first_seen)
    ) AS days_since_first_seen,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(active_addons_count_mean)
    ) AS active_addons_count_mean,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(user_pref_browser_search_region)
    ) AS user_pref_browser_search_region,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(os_version)) AS os_version,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(first_reported_country)
    ) AS first_reported_country,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(profile_group_id)) AS profile_group_id,
      -- User activity data
    MAX(max_concurrent_tab_count_max) AS max_concurrent_tab_count_max,
    SUM(tab_open_event_count_sum) AS tab_open_event_count_sum,
    SUM(active_hours_sum) AS active_hours_sum,
    SUM(subsession_hours_sum) AS subsession_hours_sum,
    SUM(sessions_started_on_this_day) AS sessions_started_on_this_day,
    SUM(total_uri_count) AS total_uri_count_sum,
      -- Search data
    SUM(organic) AS organic,
    SUM(sap) AS sap,
    SUM(unknown) AS unknown,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(ad_click) AS ad_click,
    SUM(search_with_ads) AS search_with_ads,
    SUM(total_searches) AS total_searches,
    SUM(tagged_searches) AS tagged_searches,
      -- Monthly search totals
    `moz-fx-data-shared-prod.udf.aggregate_search_map`(
      ARRAY_AGG(engine_searches)
    ) AS engine_searches
  FROM
    _derived_engine_searches
  GROUP BY
    sample_id,
    client_id
),
_current AS (
  SELECT
    * EXCEPT (days_since_first_seen, profile_group_id),
      -- In this raw table, we capture the history of activity over the past
      -- 365 days for each usage criterion as an array of bytes. The
      -- rightmost bit represents whether the user was active in the current day.
    `moz-fx-data-shared-prod.udf.bool_to_365_bits`(TRUE) AS days_seen_bytes,
    `moz-fx-data-shared-prod.udf.bool_to_365_bits`(total_searches > 0) AS days_searched_bytes,
    `moz-fx-data-shared-prod.udf.bool_to_365_bits`(
      tagged_searches > 0
    ) AS days_tagged_searched_bytes,
    `moz-fx-data-shared-prod.udf.bool_to_365_bits`(
      search_with_ads > 0
    ) AS days_searched_with_ads_bytes,
    `moz-fx-data-shared-prod.udf.bool_to_365_bits`(ad_click > 0) AS days_clicked_ads_bytes,
    `moz-fx-data-shared-prod.udf.bool_to_365_bits`(
      active_hours_sum > 0
      AND total_uri_count_sum > 0
    ) AS days_dau_bytes,
    `moz-fx-data-shared-prod.udf.int_to_365_bits`(days_since_first_seen) AS days_first_seen_bytes,
    profile_group_id
  FROM
    _grouped
),
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_last_seen_v2`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
      -- Filter out rows from yesterday that have now fallen outside the 365-day window.
    AND BIT_COUNT(`moz-fx-data-shared-prod.udf.shift_365_bits_one_day`(days_seen_bytes)) > 0
)
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_365_bits`(
      _previous.days_seen_bytes,
      _current.days_seen_bytes
    ) AS days_seen_bytes,
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_365_bits`(
      _previous.days_searched_bytes,
      _current.days_searched_bytes
    ) AS days_searched_bytes,
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_365_bits`(
      _previous.days_tagged_searched_bytes,
      _current.days_tagged_searched_bytes
    ) AS days_tagged_searched_bytes,
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_365_bits`(
      _previous.days_searched_with_ads_bytes,
      _current.days_searched_with_ads_bytes
    ) AS days_searched_with_ads_bytes,
    `moz-fx-data-shared-prod.udf.combine_adjacent_days_365_bits`(
      _previous.days_clicked_ads_bytes,
      _current.days_clicked_ads_bytes
    ) AS days_clicked_ads_bytes,
    `moz-fx-data-shared-prod.udf.coalesce_adjacent_days_365_bits`(
      _previous.days_first_seen_bytes,
      _current.days_first_seen_bytes
    ) AS days_first_seen_bytes,
    `moz-fx-data-shared-prod.udf.coalesce_adjacent_days_365_bits`(
      _previous.days_dau_bytes,
      _current.days_dau_bytes
    ) AS days_dau_bytes,
    `moz-fx-data-shared-prod.udf.add_monthly_searches`(
      _previous.engine_searches,
      _current.engine_searches,
      @submission_date
    ) AS engine_searches
  )
FROM
  _current
FULL OUTER JOIN
  _previous
  -- Include sample_id to match the clustering of the tables, which may improve
  -- join performance.
  USING (sample_id, client_id)
