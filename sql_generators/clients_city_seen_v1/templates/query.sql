 -- Query generated via sql_generators.clients_city_seen.
 -- this mimics the logic used in baseline_clients_daily_v1.
 -- some client_id do not have first_seen_geo_date since stable tables only have 2 years of data.
{% raw %}
{% if is_init() %}
{% endraw %}
WITH
{% for app_name in app_names -%}
  base_{{ app_name }} AS (
  SELECT
    submission_timestamp,
    DATE(submission_timestamp) AS submission_date,
    LOWER(client_info.client_id) AS client_id,
    sample_id,
    mozfun.glean.parse_datetime(ping_info.end_time) AS parsed_end_time,
    `moz-fx-data-shared-prod.udf.glean_timespan_seconds`( metrics.timespan.glean_baseline_duration ) AS duration,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    normalized_channel
  FROM
    `{{ project_id }}.{{ app_name }}_stable.baseline_v1`
  WHERE
    client_info.client_id IS NOT NULL
    AND sample_id = 0
    AND DATE(submission_timestamp) <= "2025-08-25"
   ),
  with_dates_{{ app_name }} AS (
  SELECT
    *,
    DATE(SAFE.TIMESTAMP_SUB(parsed_end_time, INTERVAL duration SECOND)) AS session_start_date,
    DATE(parsed_end_time) AS session_end_date,
  FROM
    base_{{ app_name }} ),
  with_date_offsets_{{ app_name }} AS (
  SELECT
    *,
    DATE_DIFF(submission_date, session_start_date, DAY) AS session_start_date_offset,
    DATE_DIFF(submission_date, session_end_date, DAY) AS session_end_date_offset,
  FROM
    with_dates_{{ app_name }} ),
  overactive_{{ app_name }} AS (
  SELECT
    submission_date,
    client_id
  FROM
    with_date_offsets_{{ app_name }}
  WHERE
    submission_date >= '2018-01-01'
  GROUP BY
    submission_date,
    client_id,
    normalized_channel
  HAVING
    COUNT(*) > 150000 ),
  windowed_{{ app_name }} AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(city) OVER w1) AS city,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(geo_subdivision1) OVER w1) AS geo_subdivision1,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(geo_subdivision2) OVER w1) AS geo_subdivision2,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(normalized_channel) OVER w1) AS normalized_channel,
  FROM
    with_date_offsets_{{ app_name }}
  LEFT JOIN
    overactive_{{ app_name }}
  USING
    (submission_date,
      client_id)
  WHERE
    overactive_{{ app_name }}.client_id IS NULL
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
  clients_daily_{{ app_name }} AS (
  SELECT
    cd.* EXCEPT (_n),
  FROM
    windowed_{{ app_name }} AS cd
  WHERE
    _n = 1 ),
clients_city_first_seen_{{ app_name }} AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    submission_date AS first_seen_geo_date,
    city AS first_seen_geo_city,
    geo_subdivision1 AS first_seen_geo_subdivision1,
    geo_subdivision2 AS first_seen_geo_subdivision2,
  FROM
    clients_daily_{{ app_name }}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY client_id, sample_id, normalized_channel ORDER BY submission_date ) = 1 ),
  clients_city_last_seen_{{ app_name }} AS (
  SELECT
    client_id,
    sample_id,
    normalized_channel,
    submission_date AS last_seen_geo_date,
    city AS last_seen_geo_city,
    geo_subdivision1 AS last_seen_geo_subdivision1,
    geo_subdivision2 AS last_seen_geo_subdivision2,
  FROM
    clients_daily_{{ app_name }}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY client_id, sample_id, normalized_channel ORDER BY submission_date DESC) = 1 ){{ "," if not loop.last }}
  {% endfor -%}
{% for app_name in app_names -%}
SELECT
  "{{ app_name }}" AS app_id,
  COALESCE(cfs.client_id, cls.client_id) AS client_id,
  COALESCE(cfs.sample_id, cls.sample_id) AS sample_id,
  COALESCE(cfs.normalized_channel, cls.normalized_channel) AS normalized_channel,
  first_seen_geo_date,
  first_seen_geo_city,
  first_seen_geo_subdivision1,
  first_seen_geo_subdivision2,
  last_seen_geo_date,
  last_seen_geo_city,
  last_seen_geo_subdivision1,
  last_seen_geo_subdivision2,
FROM
  clients_city_first_seen_{{ app_name }} cfs
FULL OUTER JOIN
  clients_city_last_seen_{{ app_name }} cls
ON
  cfs.client_id = cls.client_id
  AND cfs.sample_id = cls.sample_id
  AND cfs.normalized_channel IS NOT DISTINCT
FROM
  cls.normalized_channel --this avoids mulitple rows when normalized_channel iS NULL
{{ "UNION ALL" if not loop.last }}
{% endfor -%}
{% raw %}
{% else %}
{% endif %}
{% endraw %}
