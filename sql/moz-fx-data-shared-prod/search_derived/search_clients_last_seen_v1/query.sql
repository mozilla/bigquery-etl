WITH _derived_search_cols AS (
  SELECT
    COALESCE(udf.normalize_search_engine(engine), "Other") AS short_engine,
    COALESCE(organic, 0) + COALESCE(sap, 0) + COALESCE(unknown, 0) + COALESCE(
      tagged_sap,
      0
    ) + COALESCE(tagged_follow_on, 0) AS total_searches,
    COALESCE(tagged_sap, 0) + COALESCE(tagged_follow_on, 0) AS tagged_searches,
    COALESCE(ad_click, 0) AS ad_click,
    COALESCE(search_with_ads, 0) AS search_with_ads,
    * EXCEPT (ad_click, search_with_ads)
  FROM
    search_clients_daily_v8
  WHERE
    submission_date = @submission_date
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
    udf.mode_last(ARRAY_AGG(country)) AS country,
    udf.mode_last(ARRAY_AGG(app_version)) AS app_version,
    udf.mode_last(ARRAY_AGG(distribution_id)) AS distribution_id,
    udf.mode_last(ARRAY_AGG(locale)) AS locale,
    udf.mode_last(ARRAY_AGG(search_cohort)) AS search_cohort,
    udf.mode_last(ARRAY_AGG(addon_version)) AS addon_version,
    udf.mode_last(ARRAY_AGG(os)) AS os,
    udf.mode_last(ARRAY_AGG(channel)) AS channel,
    udf.mode_last(ARRAY_AGG(profile_creation_date)) AS profile_creation_date,
    udf.mode_last(ARRAY_AGG(default_search_engine)) AS default_search_engine,
    udf.mode_last(
      ARRAY_AGG(default_search_engine_data_load_path)
    ) AS default_search_engine_data_load_path,
    udf.mode_last(
      ARRAY_AGG(default_search_engine_data_submission_url)
    ) AS default_search_engine_data_submission_url,
    udf.mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
    udf.mode_last(ARRAY_AGG(active_addons_count_mean)) AS active_addons_count_mean,
    udf.mode_last(ARRAY_AGG(user_pref_browser_search_region)) AS user_pref_browser_search_region,
    udf.mode_last(ARRAY_AGG(os_version)) AS os_version,
      -- User activity data
    MAX(max_concurrent_tab_count_max) AS max_concurrent_tab_count_max,
    SUM(tab_open_event_count_sum) AS tab_open_event_count_sum,
    SUM(active_hours_sum) AS active_hours_sum,
    SUM(subsession_hours_sum) AS subsession_hours_sum,
    SUM(sessions_started_on_this_day) AS sessions_started_on_this_day,
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
    udf.aggregate_search_map(ARRAY_AGG(engine_searches)) AS engine_searches
  FROM
    _derived_engine_searches
  GROUP BY
    sample_id,
    client_id
),
_current AS (
  SELECT
    *,
      -- In this raw table, we capture the history of activity over the past
      -- 365 days for each usage criterion as an array of bytes. The
      -- rightmost bit represents whether the user was active in the current day.
    udf.bool_to_365_bits(TRUE) AS days_seen_bytes,
    udf.bool_to_365_bits(total_searches > 0) AS days_searched_bytes,
    udf.bool_to_365_bits(tagged_searches > 0) AS days_tagged_searched_bytes,
    udf.bool_to_365_bits(search_with_ads > 0) AS days_searched_with_ads_bytes,
    udf.bool_to_365_bits(ad_click > 0) AS days_clicked_ads_bytes,
    udf.int_to_365_bits(
      udf.days_since_created_profile_as_28_bits(
        DATE_DIFF(@submission_date, SAFE.DATE_FROM_UNIX_DATE(profile_creation_date), DAY)
      )
    ) AS days_created_profile_bytes
  FROM
    _grouped
),
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    search_clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
      -- Filter out rows from yesterday that have now fallen outside the 365-day window.
    AND BIT_COUNT(udf.shift_365_bits_one_day(days_seen_bytes)) > 0
)
SELECT
  @submission_date AS submission_date,
  IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
    udf.combine_adjacent_days_365_bits(
      _previous.days_seen_bytes,
      _current.days_seen_bytes
    ) AS days_seen_bytes,
    udf.combine_adjacent_days_365_bits(
      _previous.days_searched_bytes,
      _current.days_searched_bytes
    ) AS days_searched_bytes,
    udf.combine_adjacent_days_365_bits(
      _previous.days_tagged_searched_bytes,
      _current.days_tagged_searched_bytes
    ) AS days_tagged_searched_bytes,
    udf.combine_adjacent_days_365_bits(
      _previous.days_searched_with_ads_bytes,
      _current.days_searched_with_ads_bytes
    ) AS days_searched_with_ads_bytes,
    udf.combine_adjacent_days_365_bits(
      _previous.days_clicked_ads_bytes,
      _current.days_clicked_ads_bytes
    ) AS days_clicked_ads_bytes,
    udf.coalesce_adjacent_days_365_bits(
      _previous.days_created_profile_bytes,
      _current.days_created_profile_bytes
    ) AS days_created_profile_bytes,
    udf.add_monthly_searches(
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
