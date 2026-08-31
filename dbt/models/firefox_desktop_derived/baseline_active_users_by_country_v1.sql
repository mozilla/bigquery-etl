-- Downstream demo model: a coarser country x channel rollup built on top of
-- baseline_active_users_aggregates_v2 via ref().
SELECT
  submission_date,
  country,
  channel,
  SUM(daily_users) AS daily_users,
  SUM(weekly_users) AS weekly_users,
  SUM(monthly_users) AS monthly_users,
  SUM(dau) AS dau,
  SUM(wau) AS wau,
  SUM(mau) AS mau,
FROM
  {{ ref('baseline_active_users_aggregates_v2') }}
WHERE
  {{ submission_date_filter() }}
GROUP BY
  submission_date,
  country,
  channel
