WITH sample_cte AS (
  SELECT
    submission_date,
    os,
    SUM(dau) AS tot_dau,
    SUM(mau) AS tot_mau
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
  WHERE
    app_name = 'Firefox Desktop'
    AND submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
    AND @submission_date
  GROUP BY
    submission_date,
    os
  HAVING
    SUM(mau) > 1000
),
smoothed AS (
  SELECT
    *,
    AVG(tot_dau) OVER (
      PARTITION BY
        os
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
    ) AS nbr_Days_included
  FROM
    sample_cte
)
SELECT
  submission_date,
  os,
  tot_dau AS dau,
  tot_mau AS mau,
  smoothed_dau,
  smoothed_dau / tot_mau AS ER
FROM
  smoothed
WHERE
  nbr_days_included = 7 --only include those operating systems that have at least 1000 MAU on all 7 days
