WITH sample_cte AS (
  SELECT
    submission_date,
    CASE
      WHEN os_version IN ('Windows 10', 'Windows 11', "10.0")
        THEN 'Win10 or Win11'
      WHEN LOWER(os_version) LIKE "windows%"
        THEN os_version
      ELSE COALESCE(`mozfun.norm.windows_version_info`(os, os_version, NULL), "Unknown")
    END AS os_version,
    SUM(dau) AS tot_dau,
    SUM(mau) AS tot_mau
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
  WHERE
    app_name = 'Firefox Desktop'
    AND submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
    AND @submission_date
    AND LOWER(os) LIKE '%windows%'
    AND os_version_major + (os_version_minor / 100) > 6  --filter Windows 7+
  GROUP BY
    submission_date,
    os_version
  HAVING
    SUM(mau) > 1000
),
smoothed AS (
  SELECT
    *,
    AVG(tot_dau) OVER (
      PARTITION BY
        os_version
      ORDER BY
        submission_date
      ROWS BETWEEN
        6 PRECEDING
        AND 0 FOLLOWING
    ) AS smoothed_dau,
    COUNT(1) OVER (
      PARTITION BY
        os
      ORDER BY
        submission_date
      ROWS BETWEEN
        6 PRECEDING
        AND 0 FOLLOWING
    ) AS nbr_days_included
  FROM
    sample_cte
  WHERE
    os_version <> "Unknown"
)
SELECT
  submission_date,
  os_version AS windows_os_version,
  tot_mau AS mau,
  tot_dau AS dau,
  smoothed_dau,
  smoothed_dau / mau AS ER
FROM
  smoothed
WHERE
  nbr_days_included = 7 --only include those versions that have at least 1000 MAU on all 7 days
