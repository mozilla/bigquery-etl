 -- Query generated via sql_generators.clients_city_seen.
 -- this mimics the logic used in baseline_clients_daily_v1.
 -- some client_id do not have first_seen_geo_date since stable tables only have 2 years of data.
{% if is_init() %}
  WITH base_firefox_desktop AS (
    SELECT
      submission_timestamp,
      DATE(submission_timestamp) AS submission_date,
      LOWER(client_info.client_id) AS client_id,
      sample_id,
      mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time,
      `moz-fx-data-shared-prod.udf.glean_timespan_seconds`(
        metrics.timespan.glean_baseline_duration
      ) AS duration,
      metadata.geo.city,
      metadata.geo.subdivision1 AS geo_subdivision1,
      metadata.geo.subdivision2 AS geo_subdivision2,
      normalized_channel
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
    WHERE
      sample_id = 0
  ),
  with_dates_firefox_desktop AS (
    SELECT
      *,
      DATE(SAFE.TIMESTAMP_SUB(parsed_end_time, INTERVAL duration SECOND)) AS session_start_date,
      DATE(parsed_end_time) AS session_end_date,
    FROM
      base_firefox_desktop
  ),
  with_date_offsets_firefox_desktop AS (
    SELECT
      *,
      DATE_DIFF(submission_date, session_start_date, DAY) AS session_start_date_offset,
      DATE_DIFF(submission_date, session_end_date, DAY) AS session_end_date_offset,
    FROM
      with_dates_firefox_desktop
  ),
  overactive_firefox_desktop AS (
    SELECT
      submission_date,
      client_id
    FROM
      with_date_offsets_firefox_desktop
    WHERE
      submission_date >= "2025-08-14"
      AND submission_date <= "2025-08-19"
    GROUP BY
      submission_date,
      client_id
    HAVING
      COUNT(*) > 150000
  ),
  windowed_firefox_desktop AS (
    SELECT
      submission_date,
      client_id,
      sample_id,
      ROW_NUMBER() OVER w1_unframed AS _n,
      `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(city) OVER w1) AS city,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(geo_subdivision1) OVER w1
      ) AS geo_subdivision1,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(geo_subdivision2) OVER w1
      ) AS geo_subdivision2,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(normalized_channel) OVER w1
      ) AS normalized_channel,
    FROM
      with_date_offsets_firefox_desktop
    LEFT JOIN
      overactive_firefox_desktop
      USING (submission_date, client_id)
    WHERE
      overactive_firefox_desktop.client_id IS NULL
      AND submission_date >= "2025-08-14"
      AND submission_date <= "2025-08-19"
    WINDOW
      w1 AS (
        PARTITION BY
          sample_id,
          client_id,
          submission_date
        ORDER BY
          submission_timestamp
        ROWS BETWEEN
          UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      ),
      w1_unframed AS (
        PARTITION BY
          sample_id,
          client_id,
          submission_date
        ORDER BY
          submission_timestamp
      )
  ),
  clients_daily_firefox_desktop AS (
    SELECT
      cd.* EXCEPT (_n),
      cfs.first_seen_date,
      (cd.submission_date = cfs.first_seen_date) AS is_new_profile
    FROM
      windowed_firefox_desktop AS cd
    LEFT JOIN
      `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS cfs
      USING (client_id)
    WHERE
      _n = 1
  ),
  clients_city_first_seen_firefox_desktop AS (
    SELECT
      client_id,
      first_seen_date AS first_seen_geo_date,
      city AS first_seen_geo_city,
      geo_subdivision1 AS first_seen_geo_subdivision1,
      geo_subdivision2 AS first_seen_geo_subdivision2,
      normalized_channel
    FROM
      clients_daily_firefox_desktop
    WHERE
      is_new_profile
  ),
  clients_city_last_seen_firefox_desktop AS (
    SELECT
      submission_date AS last_seen_geo_date,
      client_id,
      city AS last_seen_geo_city,
      geo_subdivision1 AS last_seen_geo_subdivision1,
      geo_subdivision2 AS last_seen_geo_subdivision2,
      normalized_channel
    FROM
      clients_daily_firefox_desktop
    WHERE
      submission_date >= "2025-08-14"
      AND submission_date <= "2025-08-19"
      AND client_id IS NOT NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date DESC) = 1
  )
  SELECT
    'firefox_desktop' AS app_id,
    *
  FROM
    clients_city_first_seen_firefox_desktop AS fs
  FULL JOIN
    clients_city_last_seen_firefox_desktop AS ls
    USING (client_id, normalized_channel)
{% else %}
{% endif %}
