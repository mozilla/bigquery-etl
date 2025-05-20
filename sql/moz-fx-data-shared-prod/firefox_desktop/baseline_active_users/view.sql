CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
AS
SELECT
  last_seen.* EXCEPT (
    app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    distribution_id,
    attribution,
    `distribution`
  ) REPLACE(
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale
  ),
  CASE
    WHEN LOWER(IFNULL(isp, '')) = 'browserstack'
      THEN CONCAT('Firefox Desktop', ' ', isp)
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
  app_display_version AS app_version,
  `mozfun.norm.browser_version_info`(app_display_version).major_version AS app_version_major,
  `mozfun.norm.browser_version_info`(app_display_version).minor_version AS app_version_minor,
  `mozfun.norm.browser_version_info`(
    app_display_version
  ).patch_revision AS app_version_patch_revision,
  `mozfun.norm.browser_version_info`(
    app_display_version
  ).is_major_release AS app_version_is_major_release,
  normalized_channel AS channel,
  COALESCE(last_seen.distribution_id, distribution_mapping.distribution_id) AS distribution_id,
  CASE
    WHEN last_seen.distribution_id IS NOT NULL
      THEN "glean"
    WHEN distribution_mapping.distribution_id IS NOT NULL
      THEN "legacy"
    ELSE CAST(NULL AS STRING)
  END AS distribution_id_source,
  normalized_os AS os,
  --"os_grouped" is the same as "os", but we are making a choice to include it anyway
  --to make the switch from legacy sources to Glean easier since the column was in the previous view
  normalized_os AS os_grouped,
  normalized_os_version AS os_version,
  COALESCE(
    `mozfun.norm.glean_windows_version_info`(
      normalized_os,
      normalized_os_version,
      windows_build_number
    ),
    normalized_os_version
  ) AS os_version_build,
  CAST(
    `mozfun.norm.extract_version`(normalized_os_version, "major") AS INTEGER
  ) AS os_version_major,
  CAST(
    `mozfun.norm.extract_version`(normalized_os_version, "minor") AS INTEGER
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
    LOWER(IFNULL(isp, '')) <> 'browserstack'
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
