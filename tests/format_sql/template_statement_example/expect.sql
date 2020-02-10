{% include "udf.sql" %}
SELECT
  {% for column in column_list %}
    {{ column }},
  {% endfor %}
FROM
  dataset.table
WHERE
  {% if condition %}
    {{ column }} = {{ value * 2 }}
  {% else %}
    {{ column }} = {{ value * 3 }}
  {% endif %}
