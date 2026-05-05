{% set aggregate_filter_clause %}
{% if filter_version %}
  LEFT JOIN
    glam_etl.{{ prefix }}__latest_versions_v1
    USING (channel)
{% endif %}
WHERE
      -- allow for builds to be slighly ahead of the current submission date, to
      -- account for a reasonable amount of clock skew
  {{ build_date_udf }}(app_build_id) < DATE_ADD(@submission_date, INTERVAL 3 day)
      -- only keep builds from the last year
  AND {{ build_date_udf }}(app_build_id) > DATE_SUB(@submission_date, INTERVAL 365 day)
  {% if filter_version %}
    AND app_version
    BETWEEN (latest_version - {{ num_versions_to_keep }} +1)
    AND latest_version
  {% endif %}
  {% endset %}
WITH extracted_daily AS (
  SELECT
    * EXCEPT (app_version, histogram_aggregates),
    CAST(app_version AS INT64) AS app_version,
    unnested_histogram_aggregates AS histogram_aggregates
  FROM
    glam_etl.{{ prefix }}__view_clients_daily_histogram_aggregates_v1,
    UNNEST(histogram_aggregates) unnested_histogram_aggregates
  WHERE
    {% if parameterize %}
      submission_date = @submission_date
    {% else %}
      submission_date = DATE_SUB(current_date, INTERVAL 2 day)
    {% endif %}
    AND value IS NOT NULL
    AND ARRAY_LENGTH(value) > 0
),
filtered_daily AS (
  SELECT
    {{ attributes }},
    histogram_aggregates.*
  FROM
    extracted_daily {{ aggregate_filter_clause }}
),
-- re-aggregate based on the latest version
aggregated_daily AS (
  SELECT
    {{ attributes }},
    {{ metric_attributes }},
    mozfun.map.sum(ARRAY_CONCAT_AGG(mozfun.glam.histogram_filter_high_values(value))) AS value
  FROM
    filtered_daily
  GROUP BY
    {{ attributes }},
    {{ metric_attributes }}
)
SELECT
  {{ attributes }},
  ARRAY_AGG(
    STRUCT<
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
