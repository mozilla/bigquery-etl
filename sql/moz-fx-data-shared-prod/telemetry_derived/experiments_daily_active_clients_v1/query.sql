WITH desktop AS (
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
fenix AS (
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
fenix_nightly AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.baseline`
  CROSS JOIN
    UNNEST(ping_info.experiments) AS e
),
fenix_beta AS (
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
aurora AS (
  SELECT DISTINCT
    DATE(submission_timestamp) AS submission_date,
    e.key AS experiment_id,
    e.value.branch AS branch,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.baseline`
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
      desktop
    UNION ALL
    SELECT
      *
    FROM
      fenix
    UNION ALL
    SELECT
      *
    FROM
      fenix_nightly
    UNION ALL
    SELECT
      *
    FROM
      fenix_beta
    UNION ALL
    SELECT
      *
    FROM
      aurora
    UNION ALL
    SELECT
      *
    FROM
      org_mozilla_firefox
  )
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  experiment_id,
  branch
