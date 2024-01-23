WITH stage_1 AS (
  SELECT
    project,
    normalized_form_factor,
    normalized_device_name,
    normalized_device_os,
    rc_tier,
    rc_test_name,
    app_name,
    rc_value,
    rc_mean_subtest_stddev,
    test_lower_is_better,
    rc_one_week_prior_average,
    rc_two_week_prior_average,
    rc_three_week_prior_average,
    rc_four_week_prior_average,
    rc_one_week_prior_mean_stddev,
    rc_two_week_prior_mean_stddev,
    rc_three_week_prior_mean_stddev,
    rc_four_week_prior_mean_stddev,
    task_group_time,
    task_group_id,
    test_tier,
    SAFE_DIVIDE(rc_one_week_prior_average - rc_value, rc_value) AS one_week_pct_change,
    SAFE_DIVIDE(rc_two_week_prior_average - rc_value, rc_value) AS two_week_pct_change,
    SAFE_DIVIDE(rc_three_week_prior_average - rc_value, rc_value) AS three_week_pct_change,
    SAFE_DIVIDE(rc_four_week_prior_average - rc_value, rc_value) AS four_week_pct_change,
  FROM
    release_criteria_v1 AS a
  WHERE
    task_group_time = (
      SELECT
        MAX(task_group_time)
      FROM
        release_criteria_v1 AS b
      WHERE
        a.project = b.project
        AND a.platform = b.platform
        AND a.app_name = b.app_name
        AND a.rc_tier = b.rc_tier
        AND a.rc_test_name = b.rc_test_name
    )
),
stage_2 AS (
  SELECT
    a.*,
    b.rc_value AS target_value,
    b.rc_mean_subtest_stddev AS target_stddev,
    SAFE_DIVIDE(b.rc_value - a.rc_value, a.rc_value) AS target_pct_diff,
    ARRAY(
      SELECT
        `moz-fx-data-bq-performance`.udf.is_change_significant(
          a.rc_value,
          a.rc_mean_subtest_stddev,
          mean_old,
          stddev_old
        )
      FROM
        UNNEST(
          ARRAY<STRUCT<mean_old FLOAT64, stddev_old FLOAT64>>[
            (b.rc_value, b.rc_mean_subtest_stddev),
            (a.rc_one_week_prior_average, a.rc_one_week_prior_mean_stddev),
            (a.rc_two_week_prior_average, a.rc_two_week_prior_mean_stddev),
            (a.rc_three_week_prior_average, a.rc_three_week_prior_mean_stddev),
            (a.rc_four_week_prior_average, a.rc_four_week_prior_mean_stddev)
          ]
        )
    ) AS change_significant,
  FROM
    stage_1 AS a
  LEFT JOIN
    (
      SELECT DISTINCT
        task_group_id,
        project,
        normalized_device_name,
        normalized_device_os,
        rc_tier,
        rc_test_name,
        rc_value,
        rc_mean_subtest_stddev,
      FROM
        stage_1
      WHERE
        STARTS_WITH(app_name, 'target')
    ) AS b
    USING (
      task_group_id,
      project,
      normalized_device_name,
      normalized_device_os,
      rc_tier,
      rc_test_name
    )
  WHERE
    NOT STARTS_WITH(app_name, 'target')
),
stage_3 AS (
  SELECT
    *,
    ARRAY(
      SELECT
        `moz-fx-data-bq-performance`.udf.interpret_change(direction, test_lower_is_better)
      FROM
        UNNEST(change_significant)
    ) AS interpreted_change,
  FROM
    stage_2
),
stage_4 AS (
  SELECT
    *,
    change_significant[OFFSET(0)] AS is_target_diff_significant,
    change_significant[OFFSET(1)] AS is_one_week_change_significant,
    change_significant[OFFSET(2)] AS is_two_week_change_significant,
    change_significant[OFFSET(3)] AS is_three_week_change_significant,
    change_significant[OFFSET(4)] AS is_four_week_change_significant,
    interpreted_change[OFFSET(0)] AS vs_target,
    interpreted_change[OFFSET(1)] AS vs_one_week_prior,
    interpreted_change[OFFSET(2)] AS vs_two_week_prior,
    interpreted_change[OFFSET(3)] AS vs_three_week_prior,
    interpreted_change[OFFSET(4)] AS vs_four_week_prior,
    ARRAY(
      SELECT
        `moz-fx-data-bq-performance`.udf.get_formatted_comparison(
          value,
          pct,
          _interpreted_change,
          confidence
        )
      FROM
        UNNEST(
          ARRAY<STRUCT<value FLOAT64, pct FLOAT64, _interpreted_change STRING, confidence FLOAT64>>[
            (
              target_value,
              target_pct_diff,
              interpreted_change[OFFSET(0)],
              change_significant[OFFSET(0)].confidence
            ),
            (
              rc_one_week_prior_average,
              one_week_pct_change,
              interpreted_change[OFFSET(1)],
              change_significant[OFFSET(1)].confidence
            ),
            (
              rc_two_week_prior_average,
              two_week_pct_change,
              interpreted_change[OFFSET(2)],
              change_significant[OFFSET(2)].confidence
            ),
            (
              rc_three_week_prior_average,
              three_week_pct_change,
              interpreted_change[OFFSET(3)],
              change_significant[OFFSET(3)].confidence
            ),
            (
              rc_four_week_prior_average,
              four_week_pct_change,
              interpreted_change[OFFSET(4)],
              change_significant[OFFSET(4)].confidence
            )
          ]
        )
    ) AS formatted_comparison,
  FROM
    stage_3
)
SELECT
  project,
  normalized_form_factor,
  normalized_device_name,
  normalized_device_os,
  rc_tier,
  rc_test_name,
  app_name,
  ROUND(rc_value, 2) AS `current`,
  CONCAT(
    DATE(task_group_time),
    IF(`moz-fx-data-bq-performance.udf.is_stale_test`(task_group_time, test_tier), ' ⏳', '')
  ) AS last_updated,
  formatted_comparison[OFFSET(0)] AS target,
  formatted_comparison[OFFSET(1)] AS one_week_prior,
  formatted_comparison[OFFSET(2)] AS two_week_prior,
  formatted_comparison[OFFSET(3)] AS three_week_prior,
  formatted_comparison[OFFSET(4)] AS four_week_prior,
  IF(vs_target = 'regression', rc_test_name, NULL) AS vs_target_regression,
  IF(vs_target = 'improvement', rc_test_name, NULL) AS vs_target_unchanged,
  IF(vs_target = 'unchanged', rc_test_name, NULL) AS vs_target_improvement,
  IF(vs_one_week_prior = 'regression', rc_test_name, NULL) AS vs_one_week_prior_regression,
  IF(vs_one_week_prior = 'improvement', rc_test_name, NULL) AS vs_one_week_prior_unchanged,
  IF(vs_one_week_prior = 'unchanged', rc_test_name, NULL) AS vs_one_week_prior_improvement,
  IF(vs_two_week_prior = 'regression', rc_test_name, NULL) AS vs_two_week_prior_regression,
  IF(vs_two_week_prior = 'improvement', rc_test_name, NULL) AS vs_two_week_prior_unchanged,
  IF(vs_two_week_prior = 'unchanged', rc_test_name, NULL) AS vs_two_week_prior_improvement,
  IF(vs_three_week_prior = 'regression', rc_test_name, NULL) AS vs_three_week_prior_regression,
  IF(vs_three_week_prior = 'improvement', rc_test_name, NULL) AS vs_three_week_prior_unchanged,
  IF(vs_three_week_prior = 'unchanged', rc_test_name, NULL) AS vs_three_week_prior_improvement,
  IF(vs_four_week_prior = 'regression', rc_test_name, NULL) AS vs_four_week_prior_regression,
  IF(vs_four_week_prior = 'improvement', rc_test_name, NULL) AS vs_four_week_prior_unchanged,
  IF(vs_four_week_prior = 'unchanged', rc_test_name, NULL) AS vs_four_week_prior_improvement,
FROM
  stage_4
