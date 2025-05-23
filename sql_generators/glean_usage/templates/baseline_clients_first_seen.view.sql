{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ first_seen_view }}`
AS
SELECT
  *
  {% if app_name == "firefox_desktop" %}
  , 
  IF(
    LOWER(IFNULL(isp, '')) <> "browserstack"
    AND LOWER(IFNULL(coalesce(distribution_id, distribution.name), '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop,
  mozfun.norm.glean_windows_version_info(
    normalized_os,
    normalized_os_version,
    windows_build_number
  ) AS windows_version
  {% endif %}
FROM
  `{{ project_id }}.{{ daily_table }}`
WHERE
  is_new_profile
