-- Generated via ./bqetl generate experiment_monitoring
WITH org_mozilla_firefox_beta AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_fenix AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_firefox AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_ios_firefox AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_ios_firefoxbeta AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_ios_fennec AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
telemetry AS (
  SELECT DISTINCT
    submission_date,
    e.key AS experiment_id,
    e.value AS branch,
    client_id
  FROM
    telemetry.clients_daily
  CROSS JOIN
    UNNEST(experiments) AS e
),
org_mozilla_klar AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_focus AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_focus_nightly AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_focus_beta AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_ios_klar AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
org_mozilla_ios_focus AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
monitor_cirrus AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    mozfun.map.get_key(enrollment.extra, "user_id") AS user_id
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus.enrollment` AS enrollment
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
)
SELECT
  submission_date,
  experiment_id,
  branch,
  COUNT(*) AS active_clients
FROM
  (
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
      org_mozilla_ios_firefox
    UNION ALL
    SELECT
      *
    FROM
      org_mozilla_ios_firefoxbeta
    UNION ALL
    SELECT
      *
    FROM
      org_mozilla_ios_fennec
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
    UNION ALL
    SELECT
      *
    FROM
      org_mozilla_ios_klar
    UNION ALL
    SELECT
      *
    FROM
      org_mozilla_ios_focus
    UNION ALL
    SELECT
      *
    FROM
      monitor_cirrus
  )
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  experiment_id,
  branch
