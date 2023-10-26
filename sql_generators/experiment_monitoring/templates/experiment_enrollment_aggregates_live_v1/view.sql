-- Generated via ./bqetl generate experiment_monitoring
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live_v1`
AS
{% for app_dataset in applications %}
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
    `moz-fx-data-shared-prod.{{ app_dataset }}_derived.experiment_events_live_v1`
  WHERE
    window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  UNION ALL
{% endfor %}
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
