 -- Query generated via sql_generators.baseline_clients_city_seen.
 -- this mimics the logic used in baseline_clients_daily_v1.
{% raw %}
{% if is_init() %}
{% endraw %}
WITH
{% for app_id in app_id_list -%}
  base_{{ app_id }} AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    sample_id,
    mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time,
    `moz-fx-data-shared-prod.udf.glean_timespan_seconds`( metrics.timespan.glean_baseline_duration ) AS duration,
    metadata.geo.city AS city,
    metadata.geo.subdivision1 AS subdivision1,
    metadata.geo.subdivision2 AS subdivision2,
    metadata.geo.country AS country,
  FROM
    `{{ project_id }}.{{ app_id }}_stable.baseline_v1`
  WHERE
    client_info.client_id IS NOT NULL
    AND sample_id = 0
    AND DATE(submission_timestamp) <= CURRENT_DATE()
   ),
  with_dates_{{ app_id }} AS (
  SELECT
    *,
    DATE(SAFE.TIMESTAMP_SUB(parsed_end_time, INTERVAL duration SECOND)) AS session_start_date,
    DATE(parsed_end_time) AS session_end_date,
  FROM
    base_{{ app_id }} ),
  with_date_offsets_{{ app_id }} AS (
  SELECT
    *,
    DATE_DIFF(submission_date, session_start_date, DAY) AS session_start_date_offset,
    DATE_DIFF(submission_date, session_end_date, DAY) AS session_end_date_offset,
  FROM
    with_dates_{{ app_id }} ),
  overactive_{{ app_id }} AS (
  SELECT
    submission_date,
    client_id
  FROM
    with_date_offsets_{{ app_id }}
  WHERE
    submission_date >= '2018-01-01'
  GROUP BY
    submission_date,
    client_id
  HAVING
    COUNT(*) > 150000 ),
  windowed_{{ app_id }} AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    `moz-fx-data-shared-prod.udf.mode_last_retain_nulls`(
      ARRAY_AGG(STRUCT(
        city,
        subdivision1,
        subdivision2,
        country
        )
      ) OVER w1
    ) AS geo,
  FROM
    with_date_offsets_{{ app_id }}
  LEFT JOIN
    overactive_{{ app_id }}
  USING
    (submission_date,
      client_id)
  WHERE
    overactive_{{ app_id }}.client_id IS NULL
    AND submission_date >= '2018-01-01'
  WINDOW
    w1 AS (
    PARTITION BY
      sample_id,
      client_id,
      submission_date
    ORDER BY
      submission_timestamp ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING ),
    w1_unframed AS (
    PARTITION BY
      sample_id,
      client_id,
      submission_date
    ORDER BY
      submission_timestamp ) ),
  clients_daily_{{ app_id }} AS (
  SELECT
    cd.* EXCEPT (_n),
  FROM
    windowed_{{ app_id }} AS cd
  WHERE
    _n = 1 AND geo.city IS NOT NULL),
clients_city_first_seen_{{ app_id }} AS (
  SELECT
    client_id,
    sample_id,
    submission_date AS first_seen_city_date,
    geo.city AS first_seen_city,
    geo.subdivision1 AS first_seen_subdivision1,
    geo.subdivision2 AS first_seen_subdivision2,
    geo.country AS first_seen_country,
  FROM
    clients_daily_{{ app_id }}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date ) = 1 ),
  clients_city_last_seen_{{ app_id }} AS (
  SELECT
    client_id,
    sample_id,
    submission_date AS last_seen_city_date,
    geo.city AS last_seen_city,
    geo.subdivision1 AS last_seen_subdivision1,
    geo.subdivision2 AS last_seen_subdivision2,
    geo.country AS last_seen_country,
  FROM
    clients_daily_{{ app_id }}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY client_id, sample_id ORDER BY submission_date DESC) = 1 ){{ "," if not loop.last }}
  {% endfor -%}
{% for app_id in app_id_list -%}
SELECT
  "{{ app_id }}" AS app_id,
  COALESCE(cfs.client_id, cls.client_id) AS client_id,
  COALESCE(cfs.sample_id, cls.sample_id) AS sample_id,
  first_seen_city_date,
  first_seen_city,
  first_seen_subdivision1,
  first_seen_subdivision2,
  first_seen_country,
  last_seen_city_date,
  last_seen_city,
  last_seen_subdivision1,
  last_seen_subdivision2,
  last_seen_country,
FROM
  clients_city_first_seen_{{ app_id }} cfs
FULL OUTER JOIN
  clients_city_last_seen_{{ app_id }} cls
  ON cfs.client_id = cls.client_id
  AND cfs.sample_id = cls.sample_id
{{ "UNION ALL" if not loop.last }}
{% endfor -%}
{% raw %}
{% else %}
{% endraw %}
WITH
{% for app_id in app_id_list -%}
_previous_{{ app_id }} AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.{{ app_name }}_derived.{{ table_name }}`
    WHERE
      app_id = "{{ app_id }}"),
_current_windowed_{{ app_id }} AS (
  SELECT
    "{{ app_id }}" AS app_id,
    client_info.client_id AS client_id,
    sample_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    @submission_date AS submission_date,
    `moz-fx-data-shared-prod.udf.mode_last_retain_nulls`(
      ARRAY_AGG(STRUCT(
        metadata.geo.city AS city,
        metadata.geo.subdivision1 AS subdivision1,
        metadata.geo.subdivision2 AS subdivision2,
        metadata.geo.country AS country
        )
      ) OVER w1
    ) AS geo,
  FROM
    `moz-fx-data-shared-prod.{{ app_id }}_live.baseline_v1`
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
      submission_timestamp ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING ),
    w1_unframed AS (
    PARTITION BY
      sample_id,
      client_info.client_id,
      DATE(submission_timestamp)
    ORDER BY
      submission_timestamp ) ),
  _current_{{ app_id }} AS (
  SELECT
    cw.* EXCEPT (_n),
    submission_date AS first_seen_city_date,
    geo.city AS first_seen_city,
    geo.subdivision1 AS first_seen_subdivision1,
    geo.subdivision2 AS first_seen_subdivision2,
    geo.country AS first_seen_country,
    submission_date AS last_seen_city_date,
    geo.city AS last_seen_city,
    geo.subdivision1 AS last_seen_subdivision1,
    geo.subdivision2 AS last_seen_subdivision2,
    geo.country AS last_seen_country
  FROM
    _current_windowed_{{ app_id }} AS cw
  WHERE
    _n = 1
    AND geo.city IS NOT NULL){{ "," if not loop.last }}
  {% endfor -%}
{% for app_id in app_id_list -%}
SELECT
  app_id,
  client_id,
  sample_id,
IF
  (_p.client_id IS NULL,
  _c.first_seen_city_date,
  _p.first_seen_city_date
  ) AS first_seen_city_date,
IF
  (_p.client_id IS NULL,
  _c.first_seen_city,
  _p.first_seen_city
  ) AS first_seen_city,
IF
  (_p.client_id IS NULL,
  _c.first_seen_subdivision1,
  _p.first_seen_subdivision1
  ) AS first_seen_subdivision1,
IF
  (_p.client_id IS NULL,
  _c.first_seen_subdivision2,
  _p.first_seen_subdivision2
  ) AS first_seen_subdivision2,
IF
  (_p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_city_date,
  _p.last_seen_city_date
  ) AS last_seen_city_date,
IF
  (_p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_city,
  _p.last_seen_city
  ) AS last_seen_city,
IF
  (_p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_subdivision1,
  _p.last_seen_subdivision1
  ) AS last_seen_subdivision1,
IF
  (_p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_subdivision2,
  _p.last_seen_subdivision2
  ) AS last_seen_subdivision2,
FROM
  _current_{{ app_id }} AS _c
FULL JOIN
  _previous_{{ app_id }} AS _p
USING
  (client_id, sample_id, app_id)
{{ "UNION ALL" if not loop.last }}
{% endfor -%}
{% raw %}
{% endif %}
{% endraw %}
