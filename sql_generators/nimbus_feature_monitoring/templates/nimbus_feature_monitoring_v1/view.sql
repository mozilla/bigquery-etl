CREATE OR REPLACE VIEW
  `{{ view }}`
AS
(
  {% for feature, app_dataset, sql_table_name in feature_tables %}
    SELECT
      *,
      '{{ feature }}' AS feature,
      '{{ app_dataset }}' AS application,
    FROM
      `{{ sql_table_name }}`
    {{ "UNION ALL" if not loop.last }}
  {% endfor %}
)
