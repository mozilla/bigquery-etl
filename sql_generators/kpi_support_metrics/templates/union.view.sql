{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}` AS
  {% for product in products %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
      *
    FROM
      `{{ project_id }}.{{ product }}.{{ name }}`
  {% endfor %}
