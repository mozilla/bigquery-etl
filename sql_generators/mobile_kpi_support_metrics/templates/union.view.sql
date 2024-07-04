{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ target_name }}` AS
  {% for product in products %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
      *
      {% for field in product.all_possible_attribution_fields if field.exists %}
        {% if loop.first %}
          EXCEPT(
        {% endif %}
        {{ field.name }}
        {% if not loop.last %},
        {% endif %}
        {% if loop.last %}
),
{% endif %}
{% else %},
{% endfor %}
{% for field in product.all_possible_attribution_fields %}
  {% if field.exists %}
    {{ field.name }},
  {% else %}
    CAST(NULL AS {{ field.type }}) AS {{ field.name }},
  {% endif %}
{% endfor %}
{% for field in product.all_possible_attribution_fields if field.exists and field.name == "adjust_network" %}
  `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
{% else %}
  "Organic" AS paid_vs_organic,
{% endfor %}
"{{ product.name }}" AS product_name,
FROM
  `{{ project_id }}.{{ product.name }}.{{ name }}`
{% endfor %}
