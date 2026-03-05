SELECT
  TIMESTAMP(bau.submission_date) AS submission_date,
  bau.first_seen_year,
  bau.channel,
  bau.app_name,
  bau.country,
  bau.locale,
  bau.os,
  bau.os_grouped,
  bau.os_version,
  bau.os_version_major,
  bau.os_version_minor,
  bau.os_version_build,
  bau.windows_build_number,
  bau.app_version,
  bau.app_version_major,
  bau.app_version_minor,
  bau.app_version_patch_revision,
  bau.app_version_is_major_release,
  bau.is_default_browser,
  bau.distribution_id,
  bau.activity_segment,
  bau.attribution_medium,
  bau.attribution_source,
      /* Base DAU/WAU/MAU */
  COUNTIF(bau.is_daily_user) AS daily_users,
  COUNTIF(bau.is_weekly_user) AS weekly_users,
  COUNTIF(bau.is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
      /* ToU DAU/WAU/MAU (requires join) */
  COUNT(
    DISTINCT
    CASE
      WHEN tou.client_id IS NOT NULL
        AND bau.is_dau
        THEN bau.client_id
    END
  ) AS tou_daily_active_users,
  COUNT(
    DISTINCT
    CASE
      WHEN tou.client_id IS NOT NULL
        AND bau.is_wau
        THEN bau.client_id
    END
  ) AS tou_weekly_active_users,
  COUNT(
    DISTINCT
    CASE
      WHEN tou.client_id IS NOT NULL
        AND bau.is_mau
        THEN bau.client_id
    END
  ) AS tou_monthly_active_users
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` bau
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop.terms_of_use_status` tou
  ON bau.client_id = tou.client_id
  AND DATE(bau.submission_date) >= DATE(tou.submission_date)
WHERE
  bau.is_desktop
  AND DATE(bau.submission_date) = @submission_date
GROUP BY
  submission_date,
  bau.first_seen_year,
  bau.channel,
  bau.app_name,
  bau.country,
  bau.locale,
  bau.os,
  bau.os_grouped,
  bau.os_version,
  bau.os_version_major,
  bau.os_version_minor,
  bau.os_version_build,
  bau.windows_build_number,
  bau.app_version,
  bau.app_version_major,
  bau.app_version_minor,
  bau.app_version_patch_revision,
  bau.app_version_is_major_release,
  bau.is_default_browser,
  bau.distribution_id,
  bau.activity_segment,
  bau.attribution_medium,
  bau.attribution_source
