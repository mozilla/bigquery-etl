SELECT
  bau.os_grouped,
  bau.app_version,
  TIMESTAMP(bau.submission_date) AS submission_date,
  bau.country,
  CASE
    WHEN bau.country IN (
        'US',
        'CA',
        'GB',
        'DE',
        'FR',
        'ES',
        'IT',
        'BR',
        'MX',
        'IN',
        'ID',
        'CN',
        'RU'
      )
      THEN bau.country
    ELSE 'ROW'
  END AS country_segmentation,
      /* Base DAU/WAU/MAU */
  COUNT(DISTINCT CASE WHEN bau.is_dau THEN bau.client_id END) AS daily_active_users,
  COUNT(DISTINCT CASE WHEN bau.is_wau THEN bau.client_id END) AS weekly_active_users,
  COUNT(DISTINCT CASE WHEN bau.is_mau THEN bau.client_id END) AS monthly_active_users,
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
  AND bau.sample_id <= 19
  AND DATE(bau.submission_date) = @submission_date
GROUP BY
  bau.os_grouped,
  bau.app_version,
  submission_date,
  bau.country,
  country_segmentation
