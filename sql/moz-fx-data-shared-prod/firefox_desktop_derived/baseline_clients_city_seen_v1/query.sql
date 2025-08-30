 -- Query generated via sql_generators.clients_city_seen.
 -- this mimics the logic used in baseline_clients_daily_v1.
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
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
    WHERE
      client_info.client_id IS NOT NULL
      AND sample_id = 0
      AND DATE(submission_timestamp) <= "2025-08-25"
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
      submission_date >= '2018-01-01'
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
    FROM
      with_date_offsets_firefox_desktop
    LEFT JOIN
      overactive_firefox_desktop
      USING (submission_date, client_id)
    WHERE
      overactive_firefox_desktop.client_id IS NULL
      AND submission_date >= '2018-01-01'
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
    FROM
      windowed_firefox_desktop AS cd
    WHERE
      _n = 1
  ),
  clients_city_first_seen_firefox_desktop AS (
    SELECT
      client_id,
      sample_id,
      submission_date AS first_seen_geo_date,
      city AS first_seen_geo_city,
      geo_subdivision1 AS first_seen_geo_subdivision1,
      geo_subdivision2 AS first_seen_geo_subdivision2,
    FROM
      clients_daily_firefox_desktop
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date) = 1
  ),
  clients_city_last_seen_firefox_desktop AS (
    SELECT
      client_id,
      sample_id,
      submission_date AS last_seen_geo_date,
      city AS last_seen_geo_city,
      geo_subdivision1 AS last_seen_geo_subdivision1,
      geo_subdivision2 AS last_seen_geo_subdivision2,
    FROM
      clients_daily_firefox_desktop
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date DESC) = 1
  )
  SELECT
    "firefox_desktop" AS app_id,
    COALESCE(cfs.client_id, cls.client_id) AS client_id,
    COALESCE(cfs.sample_id, cls.sample_id) AS sample_id,
    first_seen_geo_date,
    first_seen_geo_city,
    first_seen_geo_subdivision1,
    first_seen_geo_subdivision2,
    last_seen_geo_date,
    last_seen_geo_city,
    last_seen_geo_subdivision1,
    last_seen_geo_subdivision2,
  FROM
    clients_city_first_seen_firefox_desktop cfs
  FULL OUTER JOIN
    clients_city_last_seen_firefox_desktop cls
    ON cfs.client_id = cls.client_id
    AND cfs.sample_id = cls.sample_id
{% else %}
  WITH _previous_firefox_desktop AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_city_seen_v1`
    WHERE
      app_id = "firefox_desktop"
  ),
  _current_windowed_firefox_desktop AS (
    SELECT
      "firefox_desktop" AS app_id,
      client_info.client_id AS client_id,
      sample_id,
      ROW_NUMBER() OVER w1_unframed AS _n,
      @submission_date AS first_seen_geo_date,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(metadata.geo.city) OVER w1
      ) AS first_seen_geo_city,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(metadata.geo.subdivision1) OVER w1
      ) AS first_seen_geo_subdivision1,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(metadata.geo.subdivision2) OVER w1
      ) AS first_seen_geo_subdivision2,
      @submission_date AS last_seen_geo_date,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(metadata.geo.city) OVER w1
      ) AS last_seen_geo_city,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(metadata.geo.subdivision1) OVER w1
      ) AS last_seen_geo_subdivision1,
      `moz-fx-data-shared-prod.udf.mode_last`(
        ARRAY_AGG(metadata.geo.subdivision2) OVER w1
      ) AS last_seen_geo_subdivision2,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_live.baseline_v1`
    WHERE
      DATE(submission_timestamp) = @submission_date
      AND sample_id = 0
    WINDOW
      w1 AS (
        PARTITION BY
          sample_id,
          client_info.client_id,
          DATE(submission_timestamp)
        ORDER BY
          submission_timestamp
        ROWS BETWEEN
          UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      ),
      w1_unframed AS (
        PARTITION BY
          sample_id,
          client_info.client_id,
          DATE(submission_timestamp)
        ORDER BY
          submission_timestamp
      )
  ),
  _current_firefox_desktop AS (
    SELECT
      cw.* EXCEPT (_n),
    FROM
      _current_windowed_firefox_desktop AS cw
    WHERE
      _n = 1
  )
  SELECT
    app_id,
    client_id,
    sample_id,
    IF(_p.client_id IS NULL, _c.first_seen_geo_date, _p.first_seen_geo_date) AS first_seen_geo_date,
    IF(_p.client_id IS NULL, _c.first_seen_geo_city, _p.first_seen_geo_city) AS first_seen_geo_city,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_geo_subdivision1,
      _p.first_seen_geo_subdivision1
    ) AS first_seen_geo_subdivision1,
    IF(
      _p.client_id IS NULL,
      _c.first_seen_geo_subdivision2,
      _p.first_seen_geo_subdivision2
    ) AS first_seen_geo_subdivision2,
    IF(
      _p.last_seen_geo_date < _c.last_seen_geo_date,
      _c.last_seen_geo_date,
      _p.last_seen_geo_date
    ) AS last_seen_geo_date,
    IF(
      _p.last_seen_geo_date < _c.last_seen_geo_date,
      _c.last_seen_geo_city,
      _p.last_seen_geo_city
    ) AS last_seen_geo_city,
    IF(
      _p.last_seen_geo_date < _c.last_seen_geo_date,
      _c.last_seen_geo_subdivision1,
      _p.last_seen_geo_subdivision1
    ) AS last_seen_geo_subdivision1,
    IF(
      _p.last_seen_geo_date < _c.last_seen_geo_date,
      _c.last_seen_geo_subdivision2,
      _p.last_seen_geo_subdivision2
    ) AS last_seen_geo_subdivision2,
  FROM
    _current_firefox_desktop AS _c
  FULL JOIN
    _previous_firefox_desktop AS _p
    USING (client_id, sample_id, app_id)
{% endif %}
