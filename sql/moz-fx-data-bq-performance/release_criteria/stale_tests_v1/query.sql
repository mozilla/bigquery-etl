SELECT DISTINCT
  project,
  platform,
  framework,
  test_tier,
  rc_test_name,
  task_group_time
FROM
  release_criteria_v1 AS a
WHERE
  `moz-fx-data-bq-performance.udf.is_stale_test`(task_group_time, test_tier)
  AND task_group_time = (
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
