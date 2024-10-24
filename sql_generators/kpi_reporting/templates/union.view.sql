{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ target_name }}`
AS
{% for product in products %}
{% if not loop.first %}
UNION ALL
{% endif %}
SELECT
  *,
  "{{ product.name }}" AS product_name,
FROM
  `{{ project_id }}.{{ product.name }}.{{ name }}`
{% endfor %}
