CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.adjust.adjust_cohort`
AS
SELECT
    date,
    period_length,
    period,
    app,
    network,
    network_token,
    country,
    os,
    device_type,
    cohort_size,
    retained_users,
    retention_rate,
    time_spent_per_users,
    time_spent_per_session,
    time_spent,
    sessions_per_user,
    sessions
FROM `moz-fx-data-shared-prod.adjust_derived.adjust_cohort_v1`
