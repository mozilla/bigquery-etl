CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__view_user_counts_v1`
AS
WITH all_clients AS (
  SELECT
    client_id,
    {{ attributes }}
  FROM `moz-fx-data-shared-prod`.{{ dataset }}.{{ prefix }}__clients_scalar_aggregates_v1
  
  UNION ALL
  
  SELECT
    client_id,
    {{ attributes }}
  FROM `moz-fx-data-shared-prod`.{{ dataset }}.{{ prefix }}__clients_histogram_aggregates_v1
)

{% for attribute_combo in attribute_combinations %}
    SELECT
        {% for attribute, in_grouping in attribute_combo %}
            {% if in_grouping %}
                {{ attribute }},
            {% else %}
                NULL AS {{ attribute }},
            {% endif %}
        {% endfor %}
        COUNT(DISTINCT client_id) as total_users
    FROM
        all_clients
    GROUP BY
        {% for attribute, in_grouping in attribute_combo if in_grouping %}
            {{ attribute }}
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
    {% if not loop.last %}
        UNION ALL
    {% endif %}
{% endfor %}