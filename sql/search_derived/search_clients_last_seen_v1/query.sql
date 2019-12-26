CREATE TEMP FUNCTION
  udf_array_drop_first_and_append(arr ANY TYPE, append ANY TYPE) AS (
    ARRAY_CONCAT(
      ARRAY(
        SELECT v
        FROM UNNEST(arr) AS v WITH OFFSET off
        WHERE off > 0
        ORDER BY off ASC),
      [append]));
CREATE TEMP FUNCTION
  udf_vector_add(a ARRAY<INT64>, b ARRAY<INT64>) AS (ARRAY(
    with a_unnested AS (
      SELECT _a, _a_off
      FROM UNNEST(a) AS _a WITH OFFSET _a_off
    ), b_unnested AS (
      SELECT _b, _b_off
      FROM UNNEST(b) AS _b WITH OFFSET _b_off
    )

    SELECT COALESCE(_a + _b, _a, _b)
    FROM a_unnested
    FULL OUTER JOIN b_unnested
      ON _a_off = _b_off
    ORDER BY COALESCE(_a_off, _b_off) ASC
  ));
CREATE TEMP FUNCTION
  udf_add_monthly_engine_searches(prev STRUCT<total_searches ARRAY<INT64>, tagged_searches ARRAY<INT64>, search_with_ads ARRAY<INT64>, ad_click ARRAY<INT64>>,
                           curr STRUCT<total_searches ARRAY<INT64>, tagged_searches ARRAY<INT64>, search_with_ads ARRAY<INT64>, ad_click ARRAY<INT64>>,
                           submission_date DATE) AS (
  IF(EXTRACT(DAY FROM submission_date) = 1,
    STRUCT(
        udf_array_drop_first_and_append(prev.total_searches, curr.total_searches[OFFSET(11)]) AS total_searches,
        udf_array_drop_first_and_append(prev.tagged_searches, curr.tagged_searches[OFFSET(11)]) AS tagged_searches,
        udf_array_drop_first_and_append(prev.search_with_ads, curr.search_with_ads[OFFSET(11)]) AS search_with_ads,
        udf_array_drop_first_and_append(prev.ad_click, curr.ad_click[OFFSET(11)]) AS ad_click
    ),
    STRUCT(
        udf_vector_add(prev.total_searches, curr.total_searches) AS total_searches,
        udf_vector_add(prev.tagged_searches, curr.tagged_searches) AS tagged_searches,
        udf_vector_add(prev.search_with_ads, curr.search_with_ads) AS search_with_ads,
        udf_vector_add(prev.ad_click, curr.ad_click) AS ad_click
    )
));
CREATE TEMP FUNCTION
  udf_array_of_12_zeroes() AS ([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
CREATE TEMP FUNCTION udf_new_monthly_engine_searches_struct() AS (
  STRUCT(
    udf_array_of_12_zeroes() AS total_searches,
    udf_array_of_12_zeroes() AS tagged_searches,
    udf_array_of_12_zeroes() AS search_with_ads,
    udf_array_of_12_zeroes() AS ad_click
  )
);
CREATE TEMP FUNCTION
  udf_add_monthly_searches(prev ARRAY<STRUCT<key STRING, value STRUCT<total_searches ARRAY<INT64>, tagged_searches ARRAY<INT64>, search_with_ads ARRAY<INT64>, ad_click ARRAY<INT64>>>>,
                           curr ARRAY<STRUCT<key STRING, value STRUCT<total_searches ARRAY<INT64>, tagged_searches ARRAY<INT64>, search_with_ads ARRAY<INT64>, ad_click ARRAY<INT64>>>>,
                           submission_date DATE) AS (ARRAY(
  WITH prev_tbl AS (
    SELECT * REPLACE(COALESCE(key, "null_engine") AS key)
    FROM UNNEST(prev)
  ), curr_tbl AS (
    SELECT * REPLACE(COALESCE(key, "null_engine") AS key)
    FROM UNNEST(curr)
  )

  SELECT
    STRUCT(
      NULLIF(key, "null_engine") AS key,
      udf_add_monthly_engine_searches(
        COALESCE(p.value, udf_new_monthly_engine_searches_struct()),
        COALESCE(c.value, udf_new_monthly_engine_searches_struct()),
        submission_date) AS value
    )
  FROM
      curr_tbl AS c
  FULL OUTER JOIN
      prev_tbl AS p
      USING (key)
));
CREATE TEMP FUNCTION
  udf_array_11_zeroes_then(val INT64)  AS (
    ARRAY [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, val]);
CREATE TEMP FUNCTION
  udf_aggregate_search_map(engine_searches_list ANY TYPE)
    AS (
      ARRAY(
        SELECT AS STRUCT
          v.key,
          STRUCT(
            udf_array_11_zeroes_then(SUM(v.value.total_searches)) AS total_searches,
            udf_array_11_zeroes_then(SUM(v.value.tagged_searches)) AS tagged_searches,
            udf_array_11_zeroes_then(SUM(v.value.search_with_ads)) AS search_with_ads,
            udf_array_11_zeroes_then(SUM(v.value.ad_click)) AS ad_click
          ) AS value
        FROM
          UNNEST(engine_searches_list) AS v
        GROUP BY
          v.key
    )
);
CREATE TEMP FUNCTION
  udf_one_as_365_bits() AS (
    CONCAT(
        REPEAT(b'\x00', 45),
        b'\x01'));
CREATE TEMP FUNCTION
  udf_zero_as_365_bits() AS (
    REPEAT(b'\x00', 46));
CREATE TEMP FUNCTION
  udf_bool_to_365_bits(val BOOLEAN) AS (
    IF(val, udf_one_as_365_bits(), udf_zero_as_365_bits()));
CREATE TEMP FUNCTION
  udf_bitmask_365() AS (
    CONCAT(
        b'\x1F',
        REPEAT(b'\xFF', 45)));
CREATE TEMP FUNCTION
  udf_shift_365_bits_one_day(x BYTES) AS (COALESCE(x << 1 & udf_bitmask_365(), udf_zero_as_365_bits()));
CREATE TEMP FUNCTION
  udf_coalesce_adjacent_days_365_bits(prev BYTES, curr BYTES) AS (
    COALESCE(
        NULLIF(udf_shift_365_bits_one_day(prev), udf_zero_as_365_bits()),
        curr,
        udf_zero_as_365_bits()
    ));
CREATE TEMP FUNCTION
  udf_combine_adjacent_days_365_bits(prev BYTES, curr BYTES) AS (
    udf_shift_365_bits_one_day(prev) | COALESCE(curr, udf_zero_as_365_bits()));
CREATE TEMP FUNCTION
  udf_days_since_created_profile_as_28_bits(days_since_created_profile INT64) AS (
  IF
    (days_since_created_profile BETWEEN 0
      AND 6,
      1 << days_since_created_profile,
      0));
CREATE TEMP FUNCTION
  udf_int_to_hex_string(value INT64) AS ((
    SELECT STRING_AGG(
        SPLIT('0123456789ABCDEF','')[OFFSET((value >> (nibbles*4)) & 0xF)], ''
        ORDER BY nibbles DESC
    )
    FROM UNNEST(generate_array(0, 16)) AS nibbles
));
CREATE TEMP FUNCTION
  udf_int_to_365_bits(value INT64) AS (
   CONCAT(REPEAT(b'\x00', 37), FROM_HEX(udf_int_to_hex_string(value))));
CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));
CREATE TEMP FUNCTION
  udf_normalize_search_engine(engine STRING) AS (
    CASE
      WHEN engine IS NULL THEN NULL
      WHEN STARTS_WITH(engine, 'google')
      OR STARTS_WITH(engine, 'Google')
      OR STARTS_WITH(engine, 'other-Google') THEN 'Google'
      WHEN STARTS_WITH(engine, 'ddg')
      OR STARTS_WITH(engine, 'duckduckgo')
      OR STARTS_WITH(engine, 'DuckDuckGo')
      OR STARTS_WITH(engine, 'other-DuckDuckGo') THEN 'DuckDuckGo'
      WHEN STARTS_WITH(engine, 'bing')
      OR STARTS_WITH(engine, 'Bing')
      OR STARTS_WITH(engine, 'other-Bing') THEN 'Bing'
      WHEN STARTS_WITH(engine, 'yandex')
      OR STARTS_WITH(engine, 'Yandex')
      OR STARTS_WITH(engine, 'other-Yandex') THEN 'Yandex'
      ELSE 'Other'
    END
  );
--
WITH
  _derived_search_cols AS (
    SELECT
      udf_normalize_search_engine(engine) AS short_engine,
      COALESCE(organic, 0) + COALESCE(sap, 0) + COALESCE(unknown, 0) + COALESCE(tagged_sap, 0) + COALESCE(tagged_follow_on, 0) AS total_searches,
      COALESCE(tagged_sap, 0) + COALESCE(tagged_follow_on, 0) AS tagged_searches,
      COALESCE(ad_click, 0) AS ad_click,
      COALESCE(search_with_ads, 0) AS search_with_ads,
      * EXCEPT (ad_click, search_with_ads)
    FROM
      search_clients_daily_v8
    WHERE
      submission_date = @submission_date
      and sample_id = 84 -- limit to 1%
  ),

  _derived_engine_searches AS (
    -- From the clients search info, make a struct
    -- that we will use for aggregation later
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
      sample_id,

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
      udf_bool_to_365_bits(TRUE) AS days_seen_bytes,
      udf_bool_to_365_bits(total_searches > 0) AS days_searched_bytes,
      udf_bool_to_365_bits(tagged_searches > 0) AS days_tagged_searched_bytes,
      udf_bool_to_365_bits(search_with_ads > 0) AS days_searched_with_ads_bytes,
      udf_bool_to_365_bits(ad_click > 0) AS days_clicked_ads_bytes,
      udf_int_to_365_bits(
        udf_days_since_created_profile_as_28_bits(
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
      AND BIT_COUNT(udf_shift_365_bits_one_day(days_seen_bytes)) > 0)

SELECT
  @submission_date AS submission_date,
IF(_current.client_id IS NOT NULL,
   _current,
   _previous).* REPLACE (
      udf_combine_adjacent_days_365_bits(
        _previous.days_seen_bytes,
        _current.days_seen_bytes
      ) AS days_seen_bytes,
      udf_combine_adjacent_days_365_bits(
        _previous.days_searched_bytes,
        _current.days_searched_bytes
      ) AS days_searched_bytes,
      udf_combine_adjacent_days_365_bits(
        _previous.days_tagged_searched_bytes,
        _current.days_tagged_searched_bytes
      ) AS days_tagged_searched_bytes,
      udf_combine_adjacent_days_365_bits(
        _previous.days_searched_with_ads_bytes,
        _current.days_searched_with_ads_bytes
      ) AS days_searched_with_ads_bytes,
      udf_combine_adjacent_days_365_bits(
        _previous.days_clicked_ads_bytes,
        _current.days_clicked_ads_bytes
      ) AS days_clicked_ads_bytes,
      udf_coalesce_adjacent_days_365_bits(
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
