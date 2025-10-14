 -- Query generated via sql_generators.baseline_clients_city_seen.
 -- this mimics the logic used in baseline_clients_daily_v1.
WITH
{% for app_id in app_id_list -%}
  base_{{ app_id }} AS (
  -- Live table dedup logic mimics copy_deduplicate
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
    metadata.geo.country AS country
    FROM
   `{{ project_id }}.{{ app_id }}_live.baseline_v1`
  WHERE
    client_info.client_id IS NOT NULL
    AND DATE(submission_timestamp) = @submission_date
    AND 'automation' NOT IN (
      SELECT TRIM(t) FROM UNNEST(SPLIT(metadata.header.x_source_tags, ',')) t
    )
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp ) = 1
   ),
  overactive_{{ app_id }} AS (
  -- Find client_ids with over 150 000 pings in a day,
  -- which could cause errors in the next step due to aggregation overflows.
  SELECT
    submission_date,
    client_id
  FROM
    base_{{ app_id }}
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id
  HAVING
    COUNT(*) > 150000 ),
  clients_daily_{{ app_id }} AS (
  SELECT
    "{{ app_id }}" AS app_id,
    submission_date,
    client_id,
    sample_id,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(STRUCT(
        city,
        subdivision1,
        subdivision2,
        country
        )
        ORDER BY submission_timestamp
      )
    ) AS geo
  FROM
    base_{{ app_id }}
  LEFT JOIN
    overactive_{{ app_id }}
  USING
    (submission_date,
      client_id)
  WHERE
    overactive_{{ app_id }}.client_id IS NULL
    -- `mode_last` can result in struct with all null values if it’s most frequent (or latest among ties).
    -- This exclude structs with all null values so there will always be one non-NULL field.
    AND COALESCE(city, subdivision1, subdivision2, country) IS NOT NULL
    AND submission_date = @submission_date
   GROUP BY
    submission_date, client_id, sample_id
 ),
  _previous_{{ app_id }} AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.{{ app_name }}_derived.{{ table_name }}`
    WHERE
      app_id = "{{ app_id }}"),
  _current_{{ app_id }} AS (
    SELECT
    app_id,
    client_id,
    sample_id,
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
    clients_daily_{{ app_id }}
    WHERE
      geo.city IS NOT NULL){{ "," if not loop.last }}
  {% endfor -%}
{% for app_id in app_id_list -%}
SELECT
-- _p.* fields are NULL for clients that are not yet captured in the baseline_city_seen derived table.
IF
  (_p.app_id IS NULL,
  _c.app_id,
  _p.app_id
  ) AS app_id,
IF
  (_p.client_id IS NULL,
  _c.client_id,
  _p.client_id
  ) AS client_id,
IF
  (_p.sample_id IS NULL,
  _c.sample_id,
  _p.sample_id
  ) AS sample_id,
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
  (_p.client_id IS NULL,
  _c.first_seen_country,
  _p.first_seen_country
  ) AS first_seen_country,
IF
  (_p.client_id IS NULL OR _p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_city_date,
  _p.last_seen_city_date
  ) AS last_seen_city_date,
IF
  (_p.client_id IS NULL OR _p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_city,
  _p.last_seen_city
  ) AS last_seen_city,
IF
  (_p.client_id IS NULL OR _p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_subdivision1,
  _p.last_seen_subdivision1
  ) AS last_seen_subdivision1,
IF
  (_p.client_id IS NULL OR _p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_subdivision2,
  _p.last_seen_subdivision2
  ) AS last_seen_subdivision2,
IF
  (_p.client_id IS NULL OR _p.last_seen_city_date < _c.last_seen_city_date,
  _c.last_seen_country,
  _p.last_seen_country
  ) AS last_seen_country
FROM
  _current_{{ app_id }} AS _c
FULL JOIN
  _previous_{{ app_id }} AS _p
USING
  (client_id, sample_id, app_id)
{{ "UNION ALL" if not loop.last }}
{% endfor -%}

