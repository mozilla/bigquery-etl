-- Generated via ./bqetl generate experiment_monitoring
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_crash_rates_live`
AS
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta_derived.experiment_crash_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_crash_aggregates_v1`
WHERE
  window_start <= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
