WITH app_data AS (
  SELECT
    Date AS date,
    mozfun.norm.firefox_android_package_name_to_channel(Package_Name) AS channel,
    Country AS code,
    SUM(Install_events) AS install_count,
    SUM(Uninstall_events) AS uninstall_count,
    SUM(Update_events) AS update_count,
  FROM
    `moz-fx-data-marketing-prod`.google_play_store.p_Installs_country_v1
  WHERE
    mozfun.norm.firefox_android_package_name_to_channel(Package_Name) IS NOT NULL
  GROUP BY
    date,
    channel,
    code
)
SELECT
  date,
  channel,
  code AS country_code,
  country_details.name AS country_name,
  country_details.region_name AS region_name,
  country_details.subregion_name AS subregion_name,
  install_count,
  uninstall_count,
  update_count,
FROM
  app_data
LEFT JOIN
  mozdata.static.country_codes_v1 AS country_details
  USING (code)
