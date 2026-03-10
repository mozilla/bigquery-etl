SELECT
  TIMESTAMP(iau.submission_date) AS submission_date,
  "firefox_ios" AS mobile_app_name,
  iau.first_seen_year,
  iau.channel,
  iau.app_name,
  iau.country,
  iau.locale,
  iau.os,
  iau.os_version,
  iau.os_version_major,
  iau.os_version_minor,
  iau.windows_build_number,
  iau.app_version,
  iau.app_version_major,
  iau.app_version_minor,
  iau.app_version_patch_revision,
  iau.app_version_is_major_release,
  iau.is_default_browser,
  iau.distribution_id,
  iau.activity_segment,
      /* Base DAU/WAU/MAU */
  COUNTIF(iau.is_daily_user) AS daily_users,
  COUNTIF(iau.is_weekly_user) AS weekly_users,
  COUNTIF(iau.is_monthly_user) AS monthly_users,
  COUNTIF(iau.is_dau) AS dau,
  COUNTIF(iau.is_wau) AS wau,
  COUNTIF(iau.is_mau) AS mau,
      /* ToU DAU/WAU/MAU (requires join) */
  COUNT(
    DISTINCT
    CASE
      WHEN ios_tou.client_id IS NOT NULL
        AND iau.is_dau
        THEN iau.client_id
    END
  ) AS tou_daily_active_users,
  COUNT(
    DISTINCT
    CASE
      WHEN ios_tou.client_id IS NOT NULL
        AND iau.is_wau
        THEN iau.client_id
    END
  ) AS tou_weekly_active_users,
  COUNT(
    DISTINCT
    CASE
      WHEN ios_tou.client_id IS NOT NULL
        AND iau.is_mau
        THEN iau.client_id
    END
  ) AS tou_monthly_active_users
FROM
  `moz-fx-data-shared-prod.firefox_ios.active_users` iau
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_ios.terms_of_use_status` ios_tou
  ON iau.client_id = ios_tou.client_id
  AND DATE(iau.submission_date) >= DATE(ios_tou.submission_date)
WHERE
  iau.is_mobile
  AND iau.submission_date = @submission_date
GROUP BY
  submission_date,
  mobile_app_name,
  iau.first_seen_year,
  iau.channel,
  iau.app_name,
  iau.country,
  iau.locale,
  iau.os,
  iau.os_version,
  iau.os_version_major,
  iau.os_version_minor,
  iau.windows_build_number,
  iau.app_version,
  iau.app_version_major,
  iau.app_version_minor,
  iau.app_version_patch_revision,
  iau.app_version_is_major_release,
  iau.is_default_browser,
  iau.distribution_id,
  iau.activity_segment
UNION ALL
SELECT
  TIMESTAMP(fau.submission_date) AS submission_date,
  "fenix" AS mobile_app_name,
  fau.first_seen_year,
  fau.channel,
  fau.app_name,
  fau.country,
  fau.locale,
  fau.os,
  fau.os_version,
  fau.os_version_major,
  fau.os_version_minor,
  fau.windows_build_number,
  fau.app_version,
  fau.app_version_major,
  fau.app_version_minor,
  fau.app_version_patch_revision,
  fau.app_version_is_major_release,
  fau.is_default_browser,
  fau.distribution_id,
  fau.activity_segment,
      /* Base DAU/WAU/MAU */
  COUNTIF(fau.is_daily_user) AS daily_users,
  COUNTIF(fau.is_weekly_user) AS weekly_users,
  COUNTIF(fau.is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
      /* ToU DAU/WAU/MAU (requires join) */
  COUNT(
    DISTINCT
    CASE
      WHEN fenix_tou.client_id IS NOT NULL
        AND fau.is_dau
        THEN fau.client_id
    END
  ) AS tou_daily_active_users,
  COUNT(
    DISTINCT
    CASE
      WHEN fenix_tou.client_id IS NOT NULL
        AND fau.is_wau
        THEN fau.client_id
    END
  ) AS tou_weekly_active_users,
  COUNT(
    DISTINCT
    CASE
      WHEN fenix_tou.client_id IS NOT NULL
        AND fau.is_mau
        THEN fau.client_id
    END
  ) AS tou_monthly_active_users
FROM
  `moz-fx-data-shared-prod.fenix.active_users` fau
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.terms_of_use_status` fenix_tou
  ON fau.client_id = fenix_tou.client_id
  AND DATE(fau.submission_date) >= DATE(fenix_tou.submission_date)
WHERE
  fau.is_mobile
  AND fau.submission_date = @submission_date
GROUP BY
  submission_date,
  mobile_app_name,
  fau.first_seen_year,
  fau.channel,
  fau.app_name,
  fau.country,
  fau.locale,
  fau.os,
  fau.os_version,
  fau.os_version_major,
  fau.os_version_minor,
  fau.windows_build_number,
  fau.app_version,
  fau.app_version_major,
  fau.app_version_minor,
  fau.app_version_patch_revision,
  fau.app_version_is_major_release,
  fau.is_default_browser,
  fau.distribution_id,
  fau.activity_segment
