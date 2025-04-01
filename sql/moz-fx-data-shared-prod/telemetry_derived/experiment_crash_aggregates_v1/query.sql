-- Generated via ./bqetl generate experiment_monitoring
WITH org_mozilla_firefox_beta AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    metrics.string.crash_process_type AS crash_process_type,
    COUNT(*) AS crash_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.crash_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
org_mozilla_fenix AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    metrics.string.crash_process_type AS crash_process_type,
    COUNT(*) AS crash_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.crash_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
org_mozilla_firefox AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    metrics.string.crash_process_type AS crash_process_type,
    COUNT(*) AS crash_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.crash_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
telemetry AS (
  SELECT
    submission_timestamp,
    unnested_experiments.key AS experiment,
    unnested_experiments.value AS branch,
    payload.process_type AS crash_process_type,
    COUNT(*) AS crash_count
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.crash_v4`
  LEFT JOIN
    UNNEST(
      ARRAY(SELECT AS STRUCT key, value.branch AS value FROM UNNEST(environment.experiments))
    ) AS unnested_experiments
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
org_mozilla_klar AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    metrics.string.crash_process_type AS crash_process_type,
    COUNT(*) AS crash_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_stable.crash_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
org_mozilla_focus AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    metrics.string.crash_process_type AS crash_process_type,
    COUNT(*) AS crash_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_stable.crash_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
org_mozilla_focus_nightly AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    metrics.string.crash_process_type AS crash_process_type,
    COUNT(*) AS crash_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.crash_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
org_mozilla_focus_beta AS (
  SELECT
    submission_timestamp,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    metrics.string.crash_process_type AS crash_process_type,
    COUNT(*) AS crash_count,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.crash_v1`
  LEFT JOIN
    UNNEST(ping_info.experiments) AS experiment
  GROUP BY
    submission_timestamp,
    experiment,
    branch,
    crash_process_type
),
all_events AS (
  SELECT
    *
  FROM
    org_mozilla_firefox_beta
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_fenix
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_firefox
  UNION ALL
  SELECT
    *
  FROM
    telemetry
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_klar
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus_nightly
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus_beta
)
SELECT
  experiment,
  branch,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    -- Aggregates event counts over 5-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
  ) AS window_start,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
  ) AS window_end,
  crash_process_type,
  SUM(crash_count) AS crash_count
FROM
  all_events
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type
