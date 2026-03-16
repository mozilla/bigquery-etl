CREATE OR REPLACE VIEW
  `{{ view }}`
AS
(
  {% for feature, full_table_name in features %}
    SELECT
      *,
      '{{ feature }}' AS feature,
    FROM
      `{{ full_table_name }}`
    {{ "UNION ALL" if not loop.last }}
  {% endfor %}
)
