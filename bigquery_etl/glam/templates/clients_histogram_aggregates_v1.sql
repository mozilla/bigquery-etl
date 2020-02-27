CREATE TEMP FUNCTION udf_merged_user_data(old_aggs ANY TYPE, new_aggs ANY TYPE)
RETURNS ARRAY<
  STRUCT<
    latest_version INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    agg_type STRING,
    sum INT64,
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
        latest_version,
        {{ metric_attributes }},
        SUM(sum) AS sum,
        udf.map_sum(ARRAY_CONCAT_AGG(value)) AS value
      FROM
        unnested
      GROUP BY
        latest_version,
        {{ metric_attributes }}
    )
    SELECT
      ARRAY_AGG((latest_version, {{ metric_attributes }}, sum, value))
    FROM
      aggregated_data
  )
);

WITH extracted_accumulated AS (
  SELECT
    *
  FROM
    glam_etl.fenix_clients_histogram_aggregates_v1
  WHERE
    sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id
),
filtered_accumulated AS (
  SELECT
    sample_id,
    -- TODO: prefix with hist_aggs
    {{ attributes }},
    histogram_aggregates
  FROM
    extracted_accumulated AS hist_aggs
  LEFT JOIN
    latest_versions
  USING
    (channel)
  WHERE
    app_version >= (latest_version - 2)
),
-- unnest the daily data
extracted_daily AS (
  SELECT
    * EXCEPT (app_version),
    CAST(app_version AS INT64) AS app_version,
    histogram_aggregates
  FROM
    glam_etl.clients_daily_histogram_aggregates_v1,
    UNNEST(histogram_aggregates) histogram_aggregates
  WHERE
    submission_date = @submission_date
    AND value IS NOT NULL
    AND ARRAY_LENGTH(value) > 0
),
filtered_daily AS (
  SELECT
    `noz-fx-data-shared-prod`.udf_js.sample_id(client_id) AS sample_id,
    {{ attributes }},
    {{ metric_attributes }},
    latest_versions,
    histogram_aggregates.*
  FROM
    extracted_daily
  LEFT JOIN
    latest_versions
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
    latest_version,
    SUM(sum) AS sum,
    udf.map_sum(ARRAY_CONCAT_AGG(value)) AS value
  FROM
    version_filtered_new
  GROUP BY
    {{ attributes }},
    {{ metric_attributes }},
    latest_version
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
        process STRING,
        agg_type STRING,
        sum INT64,
        aggregates ARRAY<STRUCT<key STRING, value INT64>>
      >(latest_version, {{ metric_attributes }}, sum, value)
    ) AS histogram_aggregates
  FROM
    aggregated_daily
  GROUP BY
    attributes
)
SELECT
  {% for attribute in attributes_list %}
    COALESCE(accumulated.{{ attribute }}, daily.{{ attribute }}) AS {{ attribute }},
  {% endfor %}
  udf_merged_user_data(accumulated, daily) AS histogram_aggregates
FROM
  filtered_accumulated AS accumulated
FULL OUTER JOIN
  transformed_daily AS daily
USING
  ({{ attributes }})
