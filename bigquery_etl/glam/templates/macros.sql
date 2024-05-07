{% macro enumerate_table_combinations(source_table, output_table, cubed_attributes, attribute_combinations, add_windows_release_sample) %}
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
)
{% endmacro %}
