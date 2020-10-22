{# Accepts: metric_attributes #}
CREATE TEMP FUNCTION udf_merged_user_data(aggs ANY TYPE)
RETURNS ARRAY<
  STRUCT<
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
        UNNEST(aggs)
    ),
    aggregated_data AS (
      SELECT AS STRUCT
        {{ metric_attributes }},
        mozfun.map.sum(ARRAY_CONCAT_AGG(value)) AS value
      FROM
        unnested
      GROUP BY
        {{ metric_attributes }}
    )
    SELECT
      ARRAY_AGG(({{ metric_attributes }}, value))
    FROM
      aggregated_data
  )
);