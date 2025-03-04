{% macro enumerate_table_combinations(source_table, output_table, cubed_attributes, attribute_combinations, add_windows_release_sample, use_sample_id) %}
-- Cross join with the attribute combinations to reduce the query complexity
-- with respect to the number of operations. A table with n rows cross joined
-- with a combination of m attributes will generate a new table with n*m rows.
-- The glob ("*") symbol can be understood as selecting all of values belonging
-- to that group.
static_combos AS (
    SELECT
        combos.*
    FROM
        UNNEST(
            ARRAY<STRUCT<
                {% for attribute in cubed_attributes %}
                    {{ attribute }} STRING {{ "," if not loop.last }}
                {% endfor %}
            >>[
                {% for attribute_combo in attribute_combinations %}
                    (
                        {% for attribute, in_grouping in attribute_combo %}
                            {{ "NULL" if in_grouping else '"*"' }}
                            {{ "," if not loop.last }}
                        {% endfor %}
                    )
                    {{ "," if not loop.last }}
                {% endfor %}
            ]
        ) as combos
),
{{ output_table }} AS (
    SELECT
        table.* EXCEPT(
            {{ cubed_attributes | join(",") }}
        ),
        {% if add_windows_release_sample %}
    -- Logic to count clients based on sampled windows release data, which started in v119.
    -- If you're changing this, then you'll also need to change
    -- clients_daily_[scalar | histogram]_aggregates
            table.os = 'Windows' AND app_version >= 119 AS sampled,
        {% endif %}
        {% for attribute in cubed_attributes %}
            COALESCE(combo.{{ attribute }}, table.{{ attribute }}) as {{ attribute }}
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    FROM
        {{ source_table }} table
    CROSS JOIN
        static_combos combo
    {% if use_sample_id %}
        WHERE
            sample_id >= @min_sample_id
            AND sample_id <= @max_sample_id
    {% endif %}
)
{% endmacro %}
{% macro filtered_data(source_table, add_windows_release_sample, use_sample_id) %}
filtered_data AS (
  SELECT
    *,
    {% if add_windows_release_sample %}
        table.os = 'Windows'
        AND app_version >= 119 AS sampled,
    {% endif %}
  FROM
    {{ source_table }} table
    {% if use_sample_id %}
        WHERE
            sample_id >= @min_sample_id
            AND sample_id <= @max_sample_id
    {% endif %}
)
{% endmacro %}
{% macro histograms_cte_select(source_table, fixed_attributes, metric_attributes, add_windows_release_sample, combination) %}
SELECT
    {% if add_windows_release_sample %}
      sampled,
    {% endif %}
    {{ fixed_attributes }},
    ARRAY(
      SELECT AS STRUCT
        {{ metric_attributes }},
        {% if add_windows_release_sample %}
        -- Logic to count clients based on sampled windows release data, which started in v119.
        -- If you're changing this, then you'll also need to change
        -- clients_daily_[scalar | histogram]_aggregates
          mozfun.glam.histogram_normalized_sum_with_original(value, IF(sampled, 10.0, 1.0)) AS aggregates,
        {% else %}
          mozfun.glam.histogram_normalized_sum_with_original(value, 1.0) AS aggregates,
        {% endif %}
      FROM unnest(histogram_aggregates)
    )AS histogram_aggregates,
    {{ combination }}
  FROM
    {{ source_table }}
{% endmacro %}
