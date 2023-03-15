-- Generated via ./bqetl generate experiment_monitoring
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live_v1`
AS
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_klar_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_focus_derived.experiment_events_live_v1`
WHERE
  window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
UNION ALL
SELECT
  type,
  experiment,
  branch,
  window_start,
  window_end,
  enroll_count,
  unenroll_count,
  graduate_count,
  update_count,
  enroll_failed_count,
  unenroll_failed_count,
  update_failed_count,
  disqualification_count,
  exposure_count,
  validation_failed_count
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_v1`
WHERE
  window_start <= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
