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
  {% for field in product.all_possible_attribution_fields if field.exists and not ((name.startswith("engagement") or name.startswith("retention")) and field.name.endswith("_timestamp")) %}
    {% if loop.first %}EXCEPT({% endif %}
    {% if not loop.last %}
    {{ field.name }},
    {% else %}
    {{ field.name }}
    {% endif %}
    {% if loop.last %}),{% endif %}
  {% else %},{% endfor %}
{% for field in product.all_possible_attribution_fields %}
  {% if field.exists %}
    {% if not ((name.startswith("engagement") or name.startswith("retention")) and field.name.endswith("_timestamp")) %}
    {{ field.name }},
    {% endif %}
  {% else %}
    {% if not ((name.startswith("engagement") or name.startswith("retention")) and field.name.endswith("_timestamp")) %}
    CAST(NULL AS {{ field.type }}) AS {{ field.name }},
    {% endif %}
  {% endif %}
{% endfor %}
"{{ product.name }}" AS product_name,
FROM
  `{{ project_id }}.{{ product.name }}.{{ name }}`
{% endfor %}
