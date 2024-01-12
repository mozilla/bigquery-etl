CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_cumulative_population_estimate_v1`
AS
WITH branches AS (
  SELECT
    normandy_slug AS experiment,
    COALESCE(branch.slug, 'null') AS branch
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  LEFT JOIN
    UNNEST(branches) AS branch
  WHERE
    normandy_slug IS NOT NULL
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
    USING (window_start, branch, experiment)
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
  SUM(cumulative_population) AS value
FROM
  (
    SELECT
      window_start AS `time`,
      branch,
      experiment,
      MIN(`cumulative_enroll_count`) - MIN(`cumulative_unenroll_count`) - MIN(
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
