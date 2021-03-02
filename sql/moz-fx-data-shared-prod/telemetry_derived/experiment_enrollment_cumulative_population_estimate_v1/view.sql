CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_cumulative_population_estimate_v1`
AS
WITH all_branches AS (
  -- We need to determine all available branches for this experiment
  SELECT DISTINCT
    branch,
    experiment
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live_v1`
),
non_null_branches AS (
  -- We need to determine if the experiment is a rollout. Rollouts do not have any branches,
  -- so unlike other experiments, null branches cannot be ignored.
  -- To determine if an experiment is a rollout, we can simply determine the number of non-null
  -- branches. If that number is 0, then we have a rollout.
  SELECT
    experiment,
    COUNTIF(branch IS NOT NULL) AS non_null_count
  FROM
    all_branches
  GROUP BY
    experiment
),
branches AS (
  SELECT
    experiment,
    IF(
      all_branches.branch IS NULL,
      IF(non_null_count = 0, 'null', all_branches.branch),
      all_branches.branch
    ) AS branch
  FROM
    all_branches
  LEFT JOIN
    non_null_branches
  USING
    (experiment)
),
branches_per_window AS (
  -- Each branch should have cumulative values for each window, even if there have been
  -- no events recorded for a branch during that time window. Then we want to take the
  -- last available values.
  SELECT
    branches.branch,
    window_start,
    experiment
  FROM
    branches
  CROSS JOIN
    (
      SELECT DISTINCT
        window_start
      FROM
        `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live_v1`
    )
),
cumulative_populations AS (
  SELECT
    window_start,
    branch,
    branches_per_window.experiment,
    SUM(enroll_count) OVER previous_rows_window AS cumulative_enroll_count,
    SUM(unenroll_count) OVER previous_rows_window AS cumulative_unenroll_count,
    SUM(graduate_count) OVER previous_rows_window AS cumulative_graduate_count,
  FROM
    branches_per_window
  LEFT JOIN
    (
      SELECT
        * EXCEPT (branch),
        IF(branch IS NULL, 'null', branch) AS branch
      FROM
        `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live_v1`
    )
  USING
    (window_start, branch, experiment)
  WINDOW
    previous_rows_window AS (
      PARTITION BY
        branches_per_window.experiment,
        branch
      ORDER BY
        window_start
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND CURRENT ROW
    )
)
SELECT
  `time`,
  experiment,
  branch,
  sum(cumulative_population) AS value
FROM
  (
    SELECT
      window_start AS `time`,
      branch,
      experiment,
      min(`cumulative_enroll_count`) - min(`cumulative_unenroll_count`) - min(
        `cumulative_graduate_count`
      ) AS cumulative_population
    FROM
      cumulative_populations
    WHERE
      branch IS NOT NULL
    GROUP BY
      1,
      2,
      3
    ORDER BY
      1
  )
GROUP BY
  1,
  2,
  3
