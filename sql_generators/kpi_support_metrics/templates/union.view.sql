{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ target_name }}` AS
  {% for product in products %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
      *
      {% for field in product.additional_attribution_fields if field.exists %}
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
{% for field in product.additional_attribution_fields %}
  {% if field.exists %}
    {{ field.name }},
  {% else %}
    CAST(NULL AS {{field.type}}) AS {{ field.name }},
  {% endif %}
{% endfor %}
FROM
  `{{ project_id }}.{{ product.name }}.{{ name }}`
{% endfor %}
