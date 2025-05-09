SELECT
  submission_date,
  app_name,
  @submission_date AS logical_dag_date,
  CURRENT_DATE() AS dag_run_date,
  SUM(CAST(is_dau AS int)) AS dau,
  SUM(CAST(is_wau AS int)) AS wau,
  SUM(CAST(is_mau AS int)) AS mau,
  SUM(CAST(is_daily_user AS int)) AS daily_users,
  SUM(CAST(is_weekly_user AS int)) AS weekly_users,
  SUM(CAST(is_monthly_user AS int)) AS monthly_users,
  SUM(CAST(is_desktop AS int)) AS desktop,
  SUM(CAST(is_mobile AS int)) AS mobile,
  COUNT(DISTINCT(client_id)) AS unique_client_ids
FROM
  `moz-fx-data-shared-prod.telemetry.active_users`
WHERE
  submission_date >= '2023-01-01'
GROUP BY
  submission_date,
  app_name,
  logical_dag_date,
  dag_run_date
