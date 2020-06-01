{% macro enumerate_table_combinations(source_table, output_table, cubed_attributes, attribute_combinations) %}
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
