CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod`.regrets_reporter_derived.regrets_reporter_summary_v1
PARTITION BY
  date
CLUSTER BY
  country,
  browser
OPTIONS
  (require_partition_filter = TRUE)
AS
WITH base_t AS (
  SELECT
    metrics.string.metadata_installation_id AS installation_id,
    DATE(submission_timestamp) AS date,
    ANY_VALUE(metrics.string.metadata_experiment_arm) AS experiment_arm,
    ANY_VALUE(metrics.string.metadata_feedback_ui_variant) AS ui_arm,
    ANY_VALUE(metadata.geo.country) AS country,
    ANY_VALUE(metadata.user_agent.browser) AS browser,
    IF(LOGICAL_OR(e.name = "video_recommended"), 1, 0) + (
      IF(LOGICAL_OR(e.name = "regret_action"), 1, 0) * 2
    ) + (IF(LOGICAL_OR(e.name = "video_played"), 1, 0) * 4) AS activities,
  FROM
    `moz-fx-data-shared-prod.regrets_reporter_ucs_stable.main_events_v1`,
    UNNEST(events) e
  WHERE
    DATE(submission_timestamp) >= DATE("2021-12-02")
  GROUP BY
    metrics.string.metadata_installation_id,
    DATE(submission_timestamp)
),
dau_t AS (
  SELECT
    COUNT(DISTINCT installation_id) AS dau,
    date,
    experiment_arm,
    ui_arm,
    country,
    browser,
    activities
  FROM
    base_t
  GROUP BY
    date,
    experiment_arm,
    ui_arm,
    country,
    browser,
    activities
  ORDER BY
    date
),
dates_t AS (
  SELECT
    date AS d
  FROM
    dau_t
  GROUP BY
    date
),
wau_t AS (
  SELECT
    COUNT(DISTINCT installation_id) AS wau,
    d AS date,
    experiment_arm,
    ui_arm,
    country,
    browser,
    activities
  FROM
    dates_t
  INNER JOIN
    base_t
    ON (date BETWEEN DATE_SUB(dates_t.d, INTERVAL 6 DAY) AND dates_t.d)
  GROUP BY
    d,
    experiment_arm,
    ui_arm,
    country,
    browser,
    activities
  ORDER BY
    d
),
new_user_base_t AS (
  SELECT
    metrics.string.metadata_installation_id AS installation_id,
    MIN(DATE(submission_timestamp)) AS date,
    ANY_VALUE(metrics.string.metadata_experiment_arm) AS experiment_arm,
    ANY_VALUE(metrics.string.metadata_feedback_ui_variant) AS ui_arm,
    ANY_VALUE(metadata.geo.country) AS country,
    ANY_VALUE(metadata.user_agent.browser) AS browser,
    IF(LOGICAL_OR(e.name = "video_recommended"), 1, 0) + (
      IF(LOGICAL_OR(e.name = "regret_action"), 1, 0) * 2
    ) + (IF(LOGICAL_OR(e.name = "video_played"), 1, 0) * 4) AS activities,
  FROM
    `moz-fx-data-shared-prod.regrets_reporter_ucs_stable.main_events_v1`,
    UNNEST(events) e
  WHERE
    DATE(submission_timestamp) >= DATE("2021-12-02")
  GROUP BY
    installation_id
),
new_user_t AS (
  SELECT
    COUNT(DISTINCT installation_id) AS new_users,
    date,
    experiment_arm,
    ui_arm,
    country,
    browser,
    activities
  FROM
    new_user_base_t
  GROUP BY
    date,
    experiment_arm,
    ui_arm,
    country,
    browser,
    activities
  ORDER BY
    date
)
SELECT
  dau,
  wau,
  new_users,
  date,
  experiment_arm,
  ui_arm,
  country,
  browser,
  activities
FROM
  dau_t
FULL JOIN
  wau_t
  USING (date, experiment_arm, ui_arm, country, browser, activities)
FULL JOIN
  new_user_t
  USING (date, experiment_arm, ui_arm, country, browser, activities)
