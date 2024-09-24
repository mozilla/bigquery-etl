{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ target_name }}`
AS
{% for product in products %}
{% if not loop.first %}
UNION ALL
{% endif %}
SELECT
  *
  {% for field in product.all_possible_attribution_fields if field.exists %}
    {% if loop.first %}EXCEPT({% endif %}
    {% if not loop.last %}
      {% if template_grain == "AGGREGATE" and not field.client_only %}
        {% if field.exists %}
        {{ field.name }},
        {% else %}
        {{ field.name }}
        {% endif %}
      {% elif template_grain == "CLIENT" %}
        {% if field.exists %}
        {{ field.name }},
        {% else %}
        {{ field.name }}
        {% endif %}
      {% endif %}
    {% endif %}
    {% if loop.last %}),{% endif %}
  {% else %},{% endfor %}
{% for field in product.all_possible_attribution_fields %}
  {% if template_grain == "AGGREGATE" and not field.client_only %}
    {% if field.exists %}
    {{ field.name }},
    {% else %}
    CAST(NULL AS {{ field.type }}) AS {{ field.name }},
    {% endif %}
  {% elif template_grain == "CLIENT" %}
    {% if field.exists %}
    {{ field.name }},
    {% else %}
    CAST(NULL AS {{ field.type }}) AS {{ field.name }},
    {% endif %}
  {% endif %}
{% endfor %}
"{{ product.name }}" AS product_name,
FROM
  `{{ project_id }}.{{ product.name }}.{{ name }}`
{% endfor %}
