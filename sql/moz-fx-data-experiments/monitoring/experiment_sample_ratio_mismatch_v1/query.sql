WITH p_values AS (
  SELECT
    v.probability,
    v.p_value
  FROM
    UNNEST(
      [
      -- based on https://people.richland.edu/james/lecture/m170/tbl-chi.html
        STRUCT(0.9 AS probability, 0.0157 AS p_value),
        STRUCT(0.75 AS probability, 0.102 AS p_value),
        STRUCT(0.5 AS probability, 0.455 AS p_value),
        STRUCT(0.25 AS probability, 1.323 AS p_value),
        STRUCT(0.1 AS probability, 2.706 AS p_value),
        STRUCT(0.05 AS probability, 3.841 AS p_value),
        STRUCT(0.025 AS probability, 5.024 AS p_value),
        STRUCT(0.01 AS probability, 6.635 AS p_value),
        STRUCT(0.005 AS probability, 7.879 AS p_value),
        STRUCT(0.002 AS probability, 9.550 AS p_value),
        STRUCT(0.001 AS probability, 10.828 AS p_value)
      ]
    ) AS v
),
active_experiments AS (
  SELECT DISTINCT
    normandy_slug AS experiment,
    b.slug AS branch,
    b.ratio
  FROM
    `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  CROSS JOIN
    UNNEST(branches) b
  WHERE
    start_date IS NOT NULL
),
srm_enrollments AS (
  WITH actual AS (
    SELECT
      experiment,
      branch,
      SUM(value) AS value
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_overall_v1`
    INNER JOIN
      active_experiments
      USING (experiment, branch)
    WHERE
      branch IS NOT NULL
    GROUP BY
      experiment,
      branch
  ),
  total_ratios AS (
    SELECT
      experiment,
      SUM(ratio) AS total_ratio
    FROM
      active_experiments
    GROUP BY
      experiment
  ),
  total AS (
    SELECT
      experiment,
      SUM(value) AS total
    FROM
      actual
    GROUP BY
      experiment
  ),
  joined AS (
    SELECT
      ae.experiment,
      value AS actual,
      total.total * (
        ratio / (SELECT total_ratio FROM total_ratios WHERE experiment = ae.experiment)
      ) AS expected
    FROM
      actual
    JOIN
      active_experiments ae
      USING (experiment, branch)
    JOIN
      total
      ON actual.experiment = total.experiment
      AND ae.experiment = total.experiment
  ),
  chisquare AS (
    SELECT
      experiment,
      SUM(SAFE_DIVIDE(POW(actual - expected, 2), expected)) AS chi
    FROM
      joined
    GROUP BY
      experiment
  ),
  chi_p_value_map AS (
    SELECT
      experiment,
      chi,
      pv.probability,
      pv.p_value
    FROM
      chisquare
    CROSS JOIN
      p_values pv
  )
  SELECT
    experiment,
    MAX(probability) AS probability,
    MIN(p_value) AS p_value
  FROM
    chi_p_value_map
  WHERE
    p_value >= chi
  GROUP BY
    experiment
),
srm_unenrollments AS (
  WITH actual AS (
    SELECT
      experiment,
      branch,
      SUM(value) AS value
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.experiment_unenrollment_overall_v1`
    INNER JOIN
      active_experiments
      USING (experiment, branch)
    WHERE
      branch IS NOT NULL
    GROUP BY
      experiment,
      branch
  ),
  total_ratios AS (
    SELECT
      experiment,
      SUM(ratio) AS total_ratio
    FROM
      active_experiments
    GROUP BY
      1
  ),
  total AS (
    SELECT
      experiment,
      SUM(value) AS total
    FROM
      actual
    GROUP BY
      experiment
  ),
  joined AS (
    SELECT
      ae.experiment,
      value AS actual,
      total.total * (
        ratio / (SELECT total_ratio FROM total_ratios WHERE experiment = ae.experiment)
      ) AS expected
    FROM
      actual
    INNER JOIN
      active_experiments ae
      USING (experiment, branch)
    JOIN
      total
      ON actual.experiment = total.experiment
      AND ae.experiment = total.experiment
  ),
  chisquare AS (
    SELECT
      experiment,
      SUM(SAFE_DIVIDE(POW(actual - expected, 2), expected)) AS chi
    FROM
      joined
    GROUP BY
      experiment
  ),
  chi_p_value_map AS (
    SELECT
      experiment,
      chi,
      pv.probability,
      pv.p_value
    FROM
      chisquare
    CROSS JOIN
      p_values pv
  )
  SELECT
    experiment,
    MAX(probability) AS probability,
    MIN(p_value) AS p_value
  FROM
    chi_p_value_map
  WHERE
    p_value >= chi
  GROUP BY
    experiment
),
srm_dau AS (
  WITH max_date_daily_clients AS (
    SELECT
      experiment_id,
      MAX(submission_date) AS submission_date
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.experiments_daily_active_clients_v1`
    GROUP BY
      experiment_id
  ),
  actual AS (
    SELECT
      dac.experiment_id AS experiment,
      dac.branch,
      dac.active_clients AS value
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.experiments_daily_active_clients_v1` dac
    INNER JOIN
      active_experiments ae
      ON ae.experiment = dac.experiment_id
      AND ae.branch = dac.branch
    JOIN
      max_date_daily_clients
      USING (experiment_id, submission_date)
    WHERE
      dac.branch IS NOT NULL
  ),
  total_ratios AS (
    SELECT
      experiment,
      SUM(ratio) AS total_ratio
    FROM
      active_experiments
    GROUP BY
      experiment
  ),
  total AS (
    SELECT
      experiment,
      SUM(value) AS total
    FROM
      actual
    GROUP BY
      experiment
  ),
  joined AS (
    SELECT
      ae.experiment,
      value AS actual,
      total.total * (
        ratio / (SELECT total_ratio FROM total_ratios WHERE experiment = ae.experiment)
      ) AS expected
    FROM
      actual
    INNER JOIN
      active_experiments ae
      USING (experiment, branch)
    JOIN
      total
      ON actual.experiment = total.experiment
      AND ae.experiment = total.experiment
  ),
  chisquare AS (
    SELECT
      experiment,
      SUM(SAFE_DIVIDE(POW(actual - expected, 2), expected)) AS chi
    FROM
      joined
    GROUP BY
      experiment
  ),
  chi_p_value_map AS (
    SELECT
      experiment,
      chi,
      pv.probability,
      pv.p_value
    FROM
      chisquare
    CROSS JOIN
      p_values pv
  )
  SELECT
    experiment,
    MAX(probability) AS probability,
    MIN(p_value) AS p_value
  FROM
    chi_p_value_map
  WHERE
    p_value >= chi
  GROUP BY
    experiment
)
SELECT
  experiment,
  enrollments.probability AS enrollments_probability,
  COALESCE(enrollments.p_value, 0) AS enrollments_p_value,
  COALESCE(enrollments.p_value, 0) < 0.1 AS enrollments_srm_likely,
  unenrollments.probability AS unenrollments_probability,
  COALESCE(unenrollments.p_value, 0) AS unenrollments_p_value,
  COALESCE(unenrollments.p_value, 0) < 0.1 AS unenrollments_srm_likely,
  dau.probability AS dau_probability,
  COALESCE(dau.p_value, 0) AS dau_p_value,
  COALESCE(dau.p_value, 0) < 0.1 AS dau_srm_likely,
FROM
  srm_enrollments enrollments
FULL OUTER JOIN
  srm_unenrollments unenrollments
  USING (experiment)
FULL OUTER JOIN
  srm_dau dau
  USING (experiment)
ORDER BY
  experiment
