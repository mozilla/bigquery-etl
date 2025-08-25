CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` (

days_since_desktop_active (description = "This is column 1"),
days_since_seen (description = "This is column 1"),
days_since_active (description = "This is column 1"),
days_since_created_profile (description = "This is column 1"),
days_since_seen_session_start (description = "This is column 1"),
days_since_seen_session_end (description = "This is column 1"),
days_seen_bits (description = "This is column 1"),
days_created_profile_bits (description = "This is column 1"),
submission_date (description = "The date when the telemetry ping is received on the server side."),
client_id (description = "A unique identifier (UUID) for the client."),
sample_id (description = "A number, 0-99, that samples by client_id and allows filtering data for analysis.
    It is a pipeline-generated artifact that should match between pings."),
first_run_date (description = "This is column 1"),
durations (description = "This is column 1"),
days_seen_session_start_bits (description = "This is column 1"),
days_seen_session_end_bits (description = "This is column 1"),
android_sdk_version (description = "The optional Android specific SDK version of the software running on this hardware device."),
locale (description = "Set of language- and/or country-based preferences for a user interface."),
city (description = "City in which the activity took place, as determined by the IP geolocation."),
country (description = "Code of the country in which the activity took place, as determined by the IP geolocation."),
app_build (description = "This is column 1"),
app_channel (description = "This is column 1"),
architecture (description = "This is column 1"),
device_manufacturer (description = "This is column 1"),
device_model (description = "This is column 1"),
telemetry_sdk_build (description = "The version of the Glean SDK at the time the ping was collected (e.g. 25.0.0)."),
first_seen_date (description = "This is column 1"),
is_new_profile (description = "This is column 1"),
isp (description = "The name of the internet service provider associated with the client's IP address."),
days_active_bits (description = "This is column 1"),
geo_subdivision (description = "This is column 1"),
profile_group_id (description = "A UUID uniquely identifying the profile group, not shared with other telemetry data."),
install_source (description = "This is column 1"),
days_desktop_active_bits (description = "This is column 1"),
windows_build_number (description = "This is column 1"),
browser_engagement_uri_count (description = "This is column 1"),
browser_engagement_active_ticks (description = "This is column 1"),
legacy_telemetry_client_id (description = "A unique identifier (UUID) for the client, that joins to legacy telemetry."),
is_default_browser (description = "A flag indicating whether the browser is set as the default browser on the client side."),
attribution_dltoken (description = "This is column 1"),
attribution_dlsource (description = "This is column 1"),
attribution_experiment (description = ""),
attribution_variation (description = ""),
attribution_ua (description = ""),
experiments (description = ""),
app_name (description = ""),
app_version (description = "User visible version string (e.g. "1.0.3") for the browser."),
app_version_major (description = "The major version of the user visible app version string for the browser, e.g. 142.1.3, has major version 142"),
app_version_minor (description = "The minor version of the user visible app version string for the browser, e.g. 142.1.3 has minor version 1"),
app_version_patch_revision (description = ""),
app_version_is_major_release (description = ""),
channel (description = "The normalized channel the application is being distributed on."),
distribution_id (description = "The distribution id associated with the install of Firefox."),
distribution_id_source (description = "")
)
OPTIONS (
  description = "TBD"
)
AS
--
SELECT
last_seen.days_since_desktop_active,
last_seen.days_since_seen,
last_seen.days_since_active,
last_seen.days_since_created_profile,
last_seen.days_since_seen_session_start,
last_seen.days_since_seen_session_end,
last_seen.days_seen_bits,
last_seen.days_created_profile_bits,
last_seen.submission_date,
last_seen.client_id,
last_seen.sample_id,
last_seen.first_run_date,
last_seen.durations,
last_seen.days_seen_session_start_bits,
last_seen.days_seen_session_end_bits,
last_seen.android_sdk_version,
COALESCE(REGEXP_EXTRACT(last_seen.locale, r'^(.+?)-'), last_seen.locale, NULL) AS locale,
IFNULL(last_seen.city,, '??') AS city,
IFNULL(last_seen.country, '??') AS country
last_seen.app_build,
last_seen.app_channel,
last_seen.architecture,
last_seen.device_manufacturer,
last_seen.device_model,
last_seen.telemetry_sdk_build,
last_seen.first_seen_date,
last_seen.is_new_profile,
last_seen.isp,
last_seen.days_active_bits,
last_seen.geo_subdivision,
last_seen.profile_group_id,
last_seen.install_source,
last_seen.days_desktop_active_bits,
last_seen.windows_build_number,
last_seen.browser_engagement_uri_count,
last_seen.browser_engagement_active_ticks,
last_seen.legacy_telemetry_client_id,
last_seen.is_default_browser,
last_seen.attribution_dltoken,
last_seen.attribution_dlsource,
last_seen.attribution_experiment,
last_seen.attribution_variation,
last_seen.attribution_ua,
last_seen.experiments,
  CASE
    WHEN LOWER(IFNULL(last_seen.isp, '')) = 'browserstack'
      THEN CONCAT('Firefox Desktop', ' ', last_seen.isp)
    WHEN LOWER(
        IFNULL(COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id), '')
      ) = 'mozillaonline'
      THEN CONCAT(
          'Firefox Desktop',
          ' ',
          COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id)
        )
    ELSE 'Firefox Desktop'
  END AS app_name,
  last_seen.app_display_version AS app_version,
  `mozfun.norm.browser_version_info`(
    last_seen.app_display_version
  ).major_version AS app_version_major,
  `mozfun.norm.browser_version_info`(
    last_seen.app_display_version
  ).minor_version AS app_version_minor,
  `mozfun.norm.browser_version_info`(
    last_seen.app_display_version
  ).patch_revision AS app_version_patch_revision,
  `mozfun.norm.browser_version_info`(
    last_seen.app_display_version
  ).is_major_release AS app_version_is_major_release,
  last_seen.normalized_channel AS channel,
  COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id) AS distribution_id,
  CASE
    WHEN last_seen.distribution_id IS NOT NULL
      THEN "glean"
    WHEN distribution_mapping.distribution_id IS NOT NULL
      THEN "legacy"
    ELSE CAST(NULL AS STRING)
  END AS distribution_id_source,
  last_seen.normalized_os AS os,
  --"os_grouped" is the same as "os", but we are making a choice to include it anyway
  --to make the switch from legacy sources to Glean easier since the column was in the previous view
  last_seen.normalized_os AS os_grouped,
  last_seen.normalized_os_version AS os_version,
  COALESCE(
    `mozfun.norm.glean_windows_version_info`(
      last_seen.normalized_os,
      last_seen.normalized_os_version,
      last_seen.windows_build_number
    ),
    last_seen.normalized_os_version
  ) AS os_version_build,
  CAST(
    `mozfun.norm.extract_version`(last_seen.normalized_os_version, "major") AS INTEGER
  ) AS os_version_major,
  CAST(
    `mozfun.norm.extract_version`(last_seen.normalized_os_version, "minor") AS INTEGER
  ) AS os_version_minor,
  CASE
    WHEN BIT_COUNT(days_desktop_active_bits)
      BETWEEN 1
      AND 6
      THEN 'infrequent_user'
    WHEN BIT_COUNT(days_desktop_active_bits)
      BETWEEN 7
      AND 13
      THEN 'casual_user'
    WHEN BIT_COUNT(days_desktop_active_bits)
      BETWEEN 14
      AND 20
      THEN 'regular_user'
    WHEN BIT_COUNT(days_desktop_active_bits) >= 21
      THEN 'core_user'
    ELSE 'other'
  END AS activity_segment,
  EXTRACT(YEAR FROM last_seen.first_seen_date) AS first_seen_year,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0, FALSE) AS is_dau,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 7, FALSE) AS is_wau,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 28, FALSE) AS is_mau,
  last_seen.attribution.campaign AS attribution_campaign,
  last_seen.attribution.content AS attribution_content,
  last_seen.attribution.medium AS attribution_medium,
  last_seen.attribution.source AS attribution_source,
  last_seen.attribution.term AS attribution_term,
  last_seen.distribution.name AS distribution_name,
  first_seen.attribution.campaign AS first_seen_attribution_campaign,
  first_seen.attribution.content AS first_seen_attribution_content,
  first_seen.attribution.medium AS first_seen_attribution_medium,
  first_seen.attribution.source AS first_seen_attribution_source,
  first_seen.attribution.term AS first_seen_attribution_term,
  first_seen.distribution.name AS first_seen_distribution_name,
  IF(
    LOWER(IFNULL(last_seen.isp, '')) <> 'browserstack'
    AND LOWER(
      IFNULL(COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id), '')
    ) <> 'mozillaonline',
    TRUE,
    FALSE
  ) AS is_desktop
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` AS last_seen
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.desktop_dau_distribution_id_history_v1` AS distribution_mapping
  USING (submission_date, client_id)
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS first_seen
  ON last_seen.client_id = first_seen.client_id
