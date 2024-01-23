{{ header }}
{% include "clients_histogram_aggregates_v1.udf.sql" %}

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
      AND app_version > (latest_version - {{ num_versions_to_keep }})
      {% endif %}
{% endset %}

CREATE TEMP FUNCTION filter_values(aggs ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS ARRAY<STRUCT<key STRING, value INT64>>
AS (
  ARRAY(
    SELECT AS STRUCT agg.key, SUM(agg.value) AS value
    FROM UNNEST(aggs) agg
    -- Prevent overflows by only keeping buckets where value is less than 2^40
    -- allowing 2^24 entries. This value was chosen somewhat abitrarily, typically
    -- the max histogram value is somewhere on the order of ~20 bits.
    -- Negative values are incorrect and should not happen but were observed,
    -- probably due to some bit flips.
    WHERE agg.value BETWEEN 0 AND POW(2, 40)
    GROUP BY agg.key
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
  {{ aggregate_filter_clause }}
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
    histogram_aggregates.*
  FROM
    extracted_daily
  {{ aggregate_filter_clause }}
),
-- re-aggregate based on the latest version
aggregated_daily AS (
  SELECT
    {{ attributes }},
    {{ metric_attributes }},
    mozfun.map.sum(ARRAY_CONCAT_AGG(filter_values(value))) AS value
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
    ARRAY_CONCAT(
      COALESCE(accumulated.histogram_aggregates, []),
      COALESCE(daily.histogram_aggregates, [])
    )
  ) AS histogram_aggregates
FROM
  filtered_accumulated AS accumulated
FULL OUTER JOIN
  transformed_daily AS daily
  USING ({{ attributes }})
