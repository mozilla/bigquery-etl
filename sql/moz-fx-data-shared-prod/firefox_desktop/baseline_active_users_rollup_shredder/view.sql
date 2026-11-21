CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users_rollup_shredder`
AS
SELECT
  submission_date,
  app_name,
  logical_dag_date,
  dag_run_date,
  dau,
  wau,
  mau,
  daily_users,
  weekly_users,
  monthly_users,
  desktop,
  unique_client_ids
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_active_users_rollup_shredder_v1`
