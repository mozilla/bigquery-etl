WITH recent_tests AS (
  SELECT DISTINCT
    task_group_id,
    framework,
    platform,
    IFNULL(normalized_test_type, '') AS test_type,
    test_name,
    IFNULL(test_extra_options, '') AS test_extra_options,
    IFNULL(subtest_name, '') AS subtest_name
  FROM
    rc_flattened_test_data_v1
  WHERE
    task_group_time >= TIMESTAMP_SUB(current_timestamp, INTERVAL 28 DAY)
),
distinct_rc AS (
  SELECT
    framework,
    platform,
    IFNULL(test_type, '') AS test_type,
    test_name,
    IFNULL(test_extra_options, '') AS test_extra_options,
    IFNULL(subtest_name, '') AS subtest_name,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT rc_test_name), '\n') AS rc_test_name,
    COUNT(*) AS defined_criteria
  FROM
    release_criteria_helper
  GROUP BY
    framework,
    platform,
    test_type,
    test_name,
    test_extra_options,
    subtest_name
)
SELECT DISTINCT
  distinct_rc.rc_test_name,
  COALESCE(distinct_rc.framework, recent_tests.framework) AS framework,
  COALESCE(distinct_rc.platform, recent_tests.platform) AS platform,
  COALESCE(distinct_rc.test_type, recent_tests.test_type) AS test_type,
  COALESCE(distinct_rc.test_name, recent_tests.test_name) AS test_name,
  COALESCE(distinct_rc.test_extra_options, recent_tests.test_extra_options) AS test_extra_options,
  COALESCE(distinct_rc.subtest_name, recent_tests.subtest_name) AS subtest_name,
  CASE
    WHEN distinct_rc.defined_criteria > 1
      THEN 'duplicate_rc'
    WHEN distinct_rc.rc_test_name IS NULL
      THEN 'missing_rc'
    WHEN recent_tests.task_group_id IS NULL
      THEN 'invalid_rc'
  END AS reason,
FROM
  distinct_rc
FULL OUTER JOIN
  recent_tests
  USING (framework, platform, test_name, test_type, test_extra_options, subtest_name)
WHERE
  distinct_rc.rc_test_name IS NULL
  OR distinct_rc.defined_criteria > 1
  OR recent_tests.task_group_id IS NULL
ORDER BY
  rc_test_name,
  framework,
  platform,
  test_type,
  test_name,
  test_extra_options
