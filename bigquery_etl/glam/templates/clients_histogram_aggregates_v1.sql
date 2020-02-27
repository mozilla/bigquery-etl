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
accumulated_filtered AS (
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
extracted_daily AS (
  SELECT
    * EXCEPT (app_version),
    CAST(app_version AS INT64) AS app_version,
    histogram_aggregates.*
  FROM
    clients_daily_histogram_aggregates_v1,
    UNNEST(histogram_aggregates) histogram_aggregates
  WHERE
    submission_date = @submission_date
    AND value IS NOT NULL
    AND ARRAY_LENGTH(value) > 0
),
daily_filtered AS (
  SELECT
    `noz-fx-data-shared-prod`.udf_js.sample_id(client_id) AS sample_id,
    -- TODO: prefix with hist_aggs
    {{ attributes }},
    {{ metric_attributes }},
    latest_versions,
    hist_aggs.value
  FROM
    filtered_aggregates AS hist_aggs
  LEFT JOIN
    latest_versions
  USING
    (channel)
  WHERE
    app_version >= (latest_version - 2)
),
daily_aggregate_by_version AS (
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
merged_aggregates AS (
  SELECT
    COALESCE(old_data.sample_id, new_data.sample_id) AS sample_id,
    COALESCE(old_data.client_id, new_data.client_id) AS client_id,
    COALESCE(old_data.os, new_data.os) AS os,
    COALESCE(old_data.app_version, new_data.app_version) AS app_version,
    COALESCE(old_data.app_build_id, new_data.app_build_id) AS app_build_id,
    COALESCE(old_data.channel, new_data.channel) AS channel,
    old_data.histogram_aggregates AS old_aggs,
    new_data.histogram_aggregates AS new_aggs
  FROM
    old_data
  FULL OUTER JOIN
    new_data
  ON
    new_data.join_key = old_data.join_key
)
SELECT
  sample_id,
  {{ attributes }},
  udf_merged_user_data(old_aggs, new_aggs) AS histogram_aggregates
FROM
  merged
