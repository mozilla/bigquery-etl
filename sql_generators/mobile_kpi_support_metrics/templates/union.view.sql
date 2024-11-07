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
  {% for field in product.all_possible_attribution_fields if field.exists and template_grain == "AGGREGATE" and not field.client_only %}
    {% if loop.first %}EXCEPT({% endif %}
      {{ field.name }}{% if not loop.last %},{% endif %}
    {% if loop.last %}){% endif %}
  {% endfor %}
  {% for field in product.all_possible_attribution_fields if field.exists and template_grain == "CLIENT" %}
    {% if loop.first %}EXCEPT({% endif %}
      {{ field.name }}{% if not loop.last %},{% endif %}
    {% if loop.last %}){% endif %}
  {% endfor %}
  ,
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
