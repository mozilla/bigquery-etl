{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ daily_view }}`
AS
SELECT
  *
 {% if app_name == "firefox_desktop" %}
, mozfun.norm.glean_windows_version_info(
    normalized_os,
    normalized_os_version,
    windows_build_number
  ) AS windows_version
 {% endif %}
FROM
  `{{ project_id }}.{{ daily_table }}`
