{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ daily_view }}`
AS
SELECT
  *,
  {% if app_name = "firefox_ios" %}
  (
    clients_daily.app_display_version = '107.2'
    AND clients_daily.submission_date >= '2023-02-01'
  ) AS is_suspicious_device_client,
  {% endif %}
FROM
  `{{ project_id }}.{{ daily_table }}`
