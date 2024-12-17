WITH sample_cte AS (
  SELECT
    submission_date,
    country,
    SUM(dau) AS tot_dau,
    SUM(mau) AS tot_mau,
  FROM
    `mozdata.telemetry.active_users_aggregates`
  WHERE
    app_name = 'Firefox Desktop'
    AND submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
    AND @submission_date
    AND country IN ('US', 'DE', 'FR', 'CA', 'GB', 'CN', 'ID', 'IN', 'RU', 'PL', 'BR', 'ES')
  GROUP BY
    submission_date,
    country
  HAVING
    SUM(mau) > 1000
),
smoothed AS (
  SELECT
    *,
    AVG(tot_dau) OVER (
      PARTITION BY
        country
      ORDER BY
        submission_date
      ROWS BETWEEN
        6 PRECEDING
        AND 0 FOLLOWING
    ) AS smoothed_dau,
    COUNT(1) OVER (
      PARTITION BY
        country
      ORDER BY
        submission_date
      ROWS BETWEEN
        6 PRECEDING
        AND 0 FOLLOWING
    ) AS nbr_days_included
  FROM
    sample_cte
)
SELECT
  submission_date,
  country,
  tot_mau AS mau,
  tot_dau AS dau,
  smoothed_dau,
  smoothed_dau / tot_mau AS ER
FROM
  smoothed
WHERE
  nbr_days_included = 7 --only include those with at least 1000 MAU on all 7 days
