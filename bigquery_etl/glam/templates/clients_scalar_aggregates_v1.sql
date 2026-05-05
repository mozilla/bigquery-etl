{{ header }}
{% include "clients_scalar_aggregates_v1.udf.sql" %}

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
      AND app_version BETWEEN (latest_version - {{ num_versions_to_keep }} + 1) AND latest_version
      {% endif %}
{% endset %}

WITH filtered_new AS (
  SELECT
    *
  FROM
    glam_etl.{{prefix}}__clients_scalar_aggregates_new_v1
),
filtered_old AS (
  SELECT
    {% for attribute in attributes_list %}
      scalar_aggs.{{ attribute }},
    {% endfor %}
    scalar_aggregates
  FROM
    {{ destination_table }} AS scalar_aggs
  {{ aggregate_filter_clause }}
),
joined_new_old AS (
  SELECT
    {% for attribute in attributes_list %}
      COALESCE(old_data.{{attribute}}, new_data.{{attribute}}) as {{attribute}},
    {% endfor %}
    COALESCE(old_data.scalar_aggregates, []) AS old_aggs,
    COALESCE(new_data.scalar_aggregates, []) AS new_aggs
  FROM
    filtered_new AS new_data
  FULL OUTER JOIN
    filtered_old AS old_data
    USING ({{ attributes }})
)
SELECT
  {{ attributes }},
  udf_merged_user_data(
    ARRAY_CONCAT(old_aggs, new_aggs)
  ) AS scalar_aggregates
FROM
  joined_new_old
