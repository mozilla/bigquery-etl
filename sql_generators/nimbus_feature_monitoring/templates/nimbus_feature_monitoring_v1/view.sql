CREATE OR REPLACE VIEW
  `{{ view }}`
AS
(
  {% for feature, application, sql_table_name in feature_tables %}
    SELECT
      *,
      '{{ feature }}' AS feature,
      '{{ application }}' AS application,
    FROM
      `{{ sql_table_name }}`
    {{ "UNION ALL" if not loop.last }}
  {% endfor %}
)
