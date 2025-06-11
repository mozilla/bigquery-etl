{{ header }} {% include "clients_histogram_aggregates_v1.udf.sql" %}
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
    extracted_accumulated {{ aggregate_filter_clause }}
),
transformed_daily AS (
  SELECT
    *
  FROM
    glam_etl.{{ prefix }}__clients_histogram_aggregates_new_v1
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
