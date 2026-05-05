CREATE OR REPLACE VIEW
  `{{ view }}`
AS
(
  {% for feature, sql_table_name in feature_tables %}
    SELECT
      *,
      '{{ feature }}' AS feature,
    FROM
      `{{ sql_table_name }}`
    {{ "UNION ALL" if not loop.last }}
  {% endfor %}
)
