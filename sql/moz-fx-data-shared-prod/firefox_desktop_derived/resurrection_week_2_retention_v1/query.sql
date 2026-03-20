WITH dau AS (
  SELECT
    client_id,
    submission_date,
    os,
    normalized_os_version,
    country,
    CASE
      WHEN submission_date = first_seen_date
        THEN TRUE
      ELSE FALSE
    END AS is_new_profile,
    LAG(submission_date) OVER (
      PARTITION BY
        client_id
      ORDER BY
        submission_date
    ) AS prev_submission_date
  FROM
    `moz-fx-data-shared-prod.telemetry.desktop_active_users`
  WHERE
    is_dau = TRUE
    AND is_desktop = TRUE
    AND submission_date
    BETWEEN DATE_SUB((DATE_SUB(@submission_date, INTERVAL 13 DAY)), INTERVAL 365 DAY)
    AND (DATE_SUB(@submission_date, INTERVAL 13 DAY))
),
active_users AS (
  SELECT
    au.submission_date,
    au.client_id,
    mozfun.bits28.retention(
      au.days_active_bits & au.days_seen_bits,
      au.submission_date
    ) AS retention_active,
  FROM
    `moz-fx-data-shared-prod.telemetry.desktop_active_users` AS au
  WHERE
    au.submission_date = @submission_date
),
final_with_days AS (
  SELECT
    d.*,
    DATE_DIFF(d.submission_date, prev_submission_date, DAY) AS num_days_since_last_seen,
    au.retention_active.day_13.active_in_week_1 AS retained_week_2,
  FROM
    dau d
  LEFT JOIN
    active_users au
    ON d.client_id = au.client_id
    AND d.submission_date = au.retention_active.day_13.metric_date
  WHERE
    d.submission_date = DATE_SUB(@submission_date, INTERVAL 13 DAY)
)
SELECT
  submission_date AS metric_date,
  @submission_date AS submission_date,
  os,
  normalized_os_version,
  country,
  CASE
    WHEN num_days_since_last_seen
      BETWEEN 29
      AND 36
      THEN ' 29-36'
    WHEN num_days_since_last_seen
      BETWEEN 37
      AND 60
      THEN ' 37-60'
    WHEN num_days_since_last_seen
      BETWEEN 61
      AND 120
      THEN ' 61-120'
    WHEN num_days_since_last_seen
      BETWEEN 121
      AND 180
      THEN '121-180'
    WHEN num_days_since_last_seen
      BETWEEN 181
      AND 365
      THEN '181-365'
    ELSE '365+'
  END AS num_days_since_last_seen,
    -- num_days_since_last_seen,
  COUNT(DISTINCT client_id) AS dau,
  COUNT(DISTINCT client_id) AS resurrections,
  COUNT(DISTINCT CASE WHEN retained_week_2 THEN client_id END) AS resurrections_retained_wk2
FROM
  final_with_days
WHERE
  NOT is_new_profile
  AND (num_days_since_last_seen >= 29 OR num_days_since_last_seen IS NULL)
GROUP BY
  1,
  2,
  3,
  4,
  5
