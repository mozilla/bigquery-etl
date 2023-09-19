SELECT
  test_run.taskGroupId AS task_group_id,
  MAX(test_run.time) OVER (PARTITION BY test_run.taskGroupId) AS task_group_time,
  test_run.platform,
  `moz-fx-data-bq-performance.udf.get_form_factor`(test_run.platform) AS normalized_form_factor,
  `moz-fx-data-bq-performance.udf.get_device_name`(test_run.platform) AS normalized_device_name,
  `moz-fx-data-bq-performance.udf.get_os_name`(test_run.platform) AS normalized_device_os,
  test_run.framework,
  test_run.project,
  test_run.tier AS test_tier,
  test_run.recordingDate AS recording_date,
  test_run.application.name AS app_name,
  test_run.application.version AS app_version,
  `moz-fx-data-bq-performance.udf.get_version_part`(
    test_run.application.version,
    0
  ) AS normalized_app_major_version,
  test_suite.name AS test_name,
   -- todo Use only test_suite.type once https://bugzilla.mozilla.org/show_bug.cgi?id=1645197 lands
  `moz-fx-data-bq-performance.udf.get_normalized_test_type`(
    test_run.framework,
    test_run.type,
    test_suite.type
  ) AS normalized_test_type,
  test_suite.lowerIsBetter AS test_lower_is_better,
  test_suite.value AS test_value,
  test_suite.unit AS test_unit,
  ARRAY_TO_STRING(
    ARRAY(SELECT x FROM UNNEST(test_suite.extraOptions) AS x ORDER BY x),
    '|'
  ) AS test_extra_options,
  subtest.name AS subtest_name,
  subtest.value AS subtest_value,
  subtest.lowerIsBetter AS subtest_lower_is_better,
  subtest.unit AS subtest_unit,
  subtest_replicate,
  subtest_replicate_offset,
  test_run.type AS test_type
FROM
  mozdata.taskclusteretl.perfherder AS test_run
LEFT JOIN
  UNNEST(suites) AS test_suite
LEFT JOIN
  UNNEST(test_suite.subtests) AS subtest
LEFT JOIN
  UNNEST(subtest.replicates) AS subtest_replicate
  WITH OFFSET AS subtest_replicate_offset
WHERE
  test_run.time >= TIMESTAMP_SUB(current_timestamp, INTERVAL(52 * 7) DAY)
  AND `moz-fx-data-bq-performance.udf.is_included_project`(test_run.project)
  AND `moz-fx-data-bq-performance.udf.is_included_framework`(test_run.framework)
  AND STRPOS(test_run.platform, '-shippable') > 0
