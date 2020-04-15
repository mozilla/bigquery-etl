{{ header }}
CREATE TEMP FUNCTION udf_merged_user_data(old_aggs ANY TYPE, new_aggs ANY TYPE)
RETURNS ARRAY<
  STRUCT<
    latest_version INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    agg_type STRING,
    value ARRAY<STRUCT<key STRING, value INT64>>
  >
> AS (
  (
    WITH unnested AS (
      SELECT
        *
      FROM
        UNNEST(old_aggs)
      UNION ALL
      SELECT
        *
      FROM
        UNNEST(new_aggs)
    ),
    aggregated_data AS (
      SELECT AS STRUCT
        {{ metric_attributes }},
        `moz-fx-data-shared-prod`.udf.map_sum(ARRAY_CONCAT_AGG(value)) AS value
      FROM
        unnested
      GROUP BY
        latest_version,
        {{ metric_attributes }}
    )
    SELECT
      ARRAY_AGG(({{ metric_attributes }}, value))
    FROM
      aggregated_data
  )
);

WITH extracted_accumulated AS (
  SELECT
    *
  FROM
    glam_etl.{{ prefix }}__clients_histogram_aggregates_v1
  {% if parameterize %}
  WHERE
    sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id
  {% endif %}
),
filtered_accumulated AS (
  SELECT
    {{ attributes }},
    histogram_aggregates
  FROM
    extracted_accumulated
  LEFT JOIN
    glam_etl.{{ prefix }}__latest_versions_v1
  USING
    (channel)
  WHERE
    app_version >= (latest_version - 2)
),
-- unnest the daily data
extracted_daily AS (
  SELECT
    * EXCEPT (app_version, histogram_aggregates),
    CAST(app_version AS INT64) AS app_version,
    unnested_histogram_aggregates as histogram_aggregates
  FROM
    glam_etl.{{ prefix }}__view_clients_daily_histogram_aggregates_v1,
    UNNEST(histogram_aggregates) unnested_histogram_aggregates
  WHERE
    {% if parameterize %}
      submission_date = @submission_date
    {% else %}
      submission_date = DATE_SUB(current_date, interval 2 day)
    {% endif %}
    AND value IS NOT NULL
    AND ARRAY_LENGTH(value) > 0
),
filtered_daily AS (
  SELECT
    {{ attributes }},
    latest_version,
    histogram_aggregates.*
  FROM
    extracted_daily
  LEFT JOIN
    glam_etl.{{ prefix }}__latest_versions_v1
  USING
    (channel)
  WHERE
    app_version >= (latest_version - 2)
),
-- re-aggregate based on the latest version
aggregated_daily AS (
  SELECT
    {{ attributes }},
    {{ metric_attributes }},
    `moz-fx-data-shared-prod`.udf.map_sum(ARRAY_CONCAT_AGG(value)) AS value
  FROM
    filtered_daily
  GROUP BY
    {{ attributes }},
    {{ metric_attributes }}
),
-- note: this seems costly, if it's just going to be unnested again
transformed_daily AS (
  SELECT
    {{ attributes }},
    ARRAY_AGG(
      STRUCT<
        latest_version INT64,
        metric STRING,
        metric_type STRING,
        key STRING,
        agg_type STRING,
        aggregates ARRAY<STRUCT<key STRING, value INT64>>
      >({{ metric_attributes }}, value)
    ) AS histogram_aggregates
  FROM
    aggregated_daily
  GROUP BY
    {{ attributes }}
)
SELECT
  {% for attribute in attributes_list %}
    COALESCE(accumulated.{{ attribute }}, daily.{{ attribute }}) AS {{ attribute }},
  {% endfor %}
  udf_merged_user_data(
    accumulated.histogram_aggregates,
    daily.histogram_aggregates
  ) AS histogram_aggregates
FROM
  filtered_accumulated AS accumulated
FULL OUTER JOIN
  transformed_daily AS daily
USING
  ({{ attributes }})
