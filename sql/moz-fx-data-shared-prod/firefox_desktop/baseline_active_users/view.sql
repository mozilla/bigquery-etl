CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
AS
SELECT
  * EXCEPT (
    app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    distribution_id,
    first_seen_client_id
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
  normalized_os_version AS os_version,
  COALESCE(
    `mozfun.norm.windows_version_info`(normalized_os, normalized_os_version, windows_build_number),
    normalized_os_version
  ) AS os_version_build,
  CAST(
    `mozfun.norm.extract_version`(
      COALESCE(
        `mozfun.norm.windows_version_info`(
          normalized_os,
          normalized_os_version,
          windows_build_number
        ),
        normalized_os_version
      ),
      "major"
    ) AS INTEGER
  ) AS os_version_major,
  CAST(
    `mozfun.norm.extract_version`(
      COALESCE(
        `mozfun.norm.windows_version_info`(
          normalized_os,
          normalized_os_version,
          windows_build_number
        ),
        normalized_os_version
      ),
      "minor"
    ) AS INTEGER
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
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` AS last_seen
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.desktop_dau_distribution_id_history_v1` AS distribution_mapping
  USING (submission_date, client_id)
LEFT JOIN
  (
    SELECT
      client_id AS first_seen_client_id,
      attribution AS first_seen_attribution,
      `distribution` AS first_seen_distribution
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1`
  ) AS first_seen
  ON last_seen.client_id = first_seen.first_seen_client_id
