-- Generated by bqetl generate desktop_crashes
CREATE OR REPLACE VIEW
  `{{ project_id }}.firefox_desktop.desktop_crashes`
AS
{% for table_name, fields in tables.items() %}
  SELECT
    {{ fields }}
  FROM
   `{{ table_name }}`
  {% if not loop.last %}
    UNION ALL
  {% endif %}
{% endfor %}