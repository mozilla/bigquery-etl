WITH
  _derived_search_cols AS (
    SELECT
      CASE
          WHEN engine IS NULL THEN 'none'
          WHEN STARTS_WITH(engine, 'google') THEN 'google'
          WHEN STARTS_WITH(engine, 'ddg') OR STARTS_WITH(engine, 'duckduckgo') THEN 'ddg'
          WHEN STARTS_WITH(engine, 'bing') THEN 'bing'
          WHEN STARTS_WITH(engine, 'yandex') THEN 'yandex'
          ELSE 'other'
      END AS short_engine,
      COALESCE(organic, 0) + COALESCE(sap, 0) + COALESCE(unknown, 0) + COALESCE(tagged_sap, 0) + COALESCE(tagged_follow_on, 0) AS total_searches,
      COALESCE(tagged_sap, 0) + COALESCE(tagged_follow_on, 0) AS tagged_searches,
      COALESCE(ad_click, 0) AS ad_click,
      COALESCE(search_with_ads, 0) AS search_with_ads,
      * EXCEPT (ad_click, search_with_ads)
    FROM
      search_clients_daily_v7
    WHERE
      submission_date_s3 = @submission_date
      and sample_id = "84"
  ),

  _derived_engine_searches AS (
    SELECT
      STRUCT(
        short_engine AS key,
        STRUCT(
          total_searches,
          tagged_searches,
          ad_click,
          search_with_ads
        ) AS value
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
      CAST(sample_id AS INT64) AS sample_id,

      -- Dimensional data
      udf_mode_last(ARRAY_AGG(country)) AS country,
      udf_mode_last(ARRAY_AGG(app_version)) AS app_version,
      udf_mode_last(ARRAY_AGG(distribution_id)) AS distribution_id,
      udf_mode_last(ARRAY_AGG(locale)) AS locale,
      udf_mode_last(ARRAY_AGG(search_cohort)) AS search_cohort,
      udf_mode_last(ARRAY_AGG(addon_version)) AS addon_version,
      udf_mode_last(ARRAY_AGG(os)) AS os,
      udf_mode_last(ARRAY_AGG(channel)) AS channel,
      udf_mode_last(ARRAY_AGG(profile_creation_date)) AS profile_creation_date,
      udf_mode_last(ARRAY_AGG(default_search_engine)) AS default_search_engine,
      udf_mode_last(ARRAY_AGG(default_search_engine_data_load_path)) AS default_search_engine_data_load_path,
      udf_mode_last(ARRAY_AGG(default_search_engine_data_submission_url)) AS default_search_engine_data_submission_url,
      udf_mode_last(ARRAY_AGG(profile_age_in_days)) AS profile_age_in_days,
      udf_mode_last(ARRAY_AGG(active_addons_count_mean)) AS active_addons_count_mean,
      udf_mode_last(ARRAY_AGG(user_pref_browser_search_region)) AS user_pref_browser_search_region,
      udf_mode_last(ARRAY_AGG(os_version)) AS os_version,

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
      udf_aggregate_search_map(ARRAY_AGG(engine_searches)) AS engine_searches

    FROM _derived_engine_searches
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
    udf_bool_to_bytes(TRUE) AS days_seen_bytes,
    udf_bool_to_bytes(total_searches > 0) AS days_searched_bytes,
    udf_bool_to_bytes(tagged_searches > 0) AS days_tagged_searched_bytes,
    udf_bool_to_bytes(search_with_ads > 0) AS days_searched_with_ads_bytes,
    udf_bool_to_bytes(ad_click > 0) AS days_clicked_ads_bytes,
    udf_int_to_bytes(
      udf_bits_from_days_since_created_profile(
        DATE_DIFF(@submission_date,
                  SAFE.DATE_FROM_UNIX_DATE(profile_creation_date),
                  DAY))) AS days_created_profile_bytes
  FROM
    _grouped),

  _previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    search_clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 365-day window.
    AND udf_combine_adjacent_days_bytes(days_seen_bytes, udf_bool_to_bytes(FALSE)) != udf_zeroed_365_days_bytes())

SELECT
  @submission_date AS submission_date,
IF(_current.client_id IS NOT NULL,
   _current,
   _previous).* REPLACE (
      udf_combine_adjacent_days_bytes(
        _previous.days_seen_bytes,
        _current.days_seen_bytes
      ) AS days_seen_bytes,
      udf_combine_adjacent_days_bytes(
        _previous.days_searched_bytes,
        _current.days_searched_bytes
      ) AS days_searched_bytes,
      udf_combine_adjacent_days_bytes(
        _previous.days_tagged_searched_bytes,
        _current.days_tagged_searched_bytes
      ) AS days_tagged_searched_bytes,
      udf_combine_adjacent_days_bytes(
        _previous.days_searched_with_ads_bytes,
        _current.days_searched_with_ads_bytes
      ) AS days_searched_with_ads_bytes,
      udf_combine_adjacent_days_bytes(
        _previous.days_clicked_ads_bytes,
        _current.days_clicked_ads_bytes
      ) AS days_clicked_ads_bytes,
      udf_coalesce_adjacent_days_bytes(
        _previous.days_created_profile_bytes,
        _current.days_created_profile_bytes
      ) AS days_created_profile_bytes,
      udf_add_monthly_searches(
        _previous.engine_searches,
        _current.engine_searches,
        @submission_date
      ) AS engine_searches)
FROM
  _current
FULL OUTER JOIN
  _previous
USING
  -- Include sample_id to match the clustering of the tables, which may improve
  -- join performance.
  (sample_id,
    client_id)
