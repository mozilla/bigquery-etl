WITH rc_included_tests AS (
  -- Filter and correct table that defines the aggregation for release criteria
  SELECT
    * REPLACE (
      IFNULL(rc_tier, 5) AS rc_tier,
      IFNULL(rc_test_aggregator, 'use_existing') AS rc_test_aggregator,
      IFNULL(rc_subtest_aggregator, 'use_existing') AS rc_subtest_aggregator,
      IFNULL(rc_replicate_aggregator, 'use_existing') AS rc_replicate_aggregator,
      IFNULL(rc_ignore_first_replicates, 0) AS rc_ignore_first_replicates,
      IFNULL(rc_target_type, 'none') AS rc_target_type,
      IFNULL(test_extra_options, '') AS test_extra_options
    )
  FROM
    release_criteria_helper
  WHERE
    rc_test_name IS NOT NULL
    AND rc_test_name != 'exclude'
),
rc_test_data AS (
  -- Join the flattened test data with the table that defines the aggregation for release criteria
  SELECT
    flattened.* EXCEPT (subtest_replicate_offset, test_type),
    CONCAT(
      flattened.task_group_id,
      flattened.project,
      flattened.platform,
      COALESCE(flattened.app_name, ''),
      included_rc.rc_tier,
      included_rc.rc_test_name
    ) AS rc_test_aggregation_key,
    included_rc.rc_test_name,
    included_rc.rc_tier,
    included_rc.rc_test_aggregator,
    included_rc.rc_subtest_aggregator,
    included_rc.rc_replicate_aggregator,
    included_rc.rc_ignore_first_replicates,
    included_rc.rc_target_type,
    included_rc.rc_target_value,
    included_rc.rc_target_app,
  FROM
    rc_flattened_test_data_v1 AS flattened
  LEFT JOIN
    rc_included_tests AS included_rc
    ON flattened.framework = included_rc.framework
    AND flattened.platform = included_rc.platform
    AND flattened.normalized_test_type = included_rc.test_type
    AND flattened.test_name = included_rc.test_name
    AND flattened.test_extra_options = included_rc.test_extra_options
    AND flattened.subtest_name = included_rc.subtest_name
  WHERE
    rc_ignore_first_replicates < (subtest_replicate_offset + 1)
),
kurtosis_parts AS (
  SELECT
    * EXCEPT (subtest_replicate),
    CASE
      rc_replicate_aggregator
      WHEN 'use_existing'
        THEN subtest_value
      WHEN 'mean'
        THEN AVG(subtest_replicate) OVER (replicate_window)
      WHEN 'median'
        THEN PERCENTILE_CONT(subtest_replicate, 0.5) OVER (replicate_window)
      WHEN 'geomean'
        -- Adding 1 to the replicate value then subtracting 1 from the result ensures that a value is always returned.
        -- Maintaining here to be consistent with how the perf test framework calculates geomean: https://searchfox.org/mozilla-central/source/testing/talos/talos/filter.py#174
        THEN EXP(
            AVG(LN(IF(rc_replicate_aggregator = 'geomean', subtest_replicate + 1, NULL))) OVER (
              replicate_window
            )
          ) - 1
      WHEN 'max'
        THEN MAX(subtest_replicate) OVER (replicate_window)
      WHEN 'min'
        THEN MIN(subtest_replicate) OVER (replicate_window)
      WHEN 'sum'
        THEN SUM(subtest_replicate) OVER (replicate_window)
      ELSE ERROR(CONCAT('Unknown replicate aggregator: ', rc_replicate_aggregator))
    END AS rc_subtest_value,
    AVG(subtest_replicate) OVER (replicate_window) AS rc_replicate_mean,
    STDDEV_SAMP(subtest_replicate) OVER (replicate_window) AS rc_replicate_stddev,
    COUNT(subtest_replicate) OVER (replicate_window) AS rc_replicate_count,
    TO_JSON_STRING(ARRAY_AGG(subtest_replicate) OVER (replicate_window)) AS replicate_values_json,
    SUM(1.0 * subtest_replicate) OVER (replicate_window) AS rx,
    SUM(POWER(1.0 * subtest_replicate, 2)) OVER (replicate_window) AS rx2,
    SUM(POWER(1.0 * subtest_replicate, 3)) OVER (replicate_window) AS rx3,
    SUM(POWER(1.0 * subtest_replicate, 4)) OVER (replicate_window) AS rx4,
  FROM
    rc_test_data
  WINDOW
    replicate_window AS (
      PARTITION BY
        rc_test_aggregation_key,
        test_name,
        subtest_name
    )
),
subtests AS (
  -- Aggregate the in-scope replicates and calculate summary statistics
  SELECT DISTINCT
    * EXCEPT (rx, rx2, rx3, rx4),
    `moz-fx-data-bq-performance.udf.skewness`(
      rc_replicate_count,
      rc_replicate_mean,
      rc_replicate_stddev,
      rx,
      rx2,
      rx3
    ) AS rc_replicate_skewness,
    `moz-fx-data-bq-performance.udf.kurtosis`(
      rc_replicate_count,
      rc_replicate_mean,
      rc_replicate_stddev,
      rx,
      rx2,
      rx3,
      rx4
    ) AS rc_replicate_kurtosis,
  FROM
    kurtosis_parts
),
tests AS (
  -- Aggregate the in-scope sub tests and calculate the stddev and count
  SELECT DISTINCT
    * EXCEPT (
      rc_subtest_value,
      rc_replicate_mean,
      rc_replicate_stddev,
      rc_replicate_count,
      test_tier,
      subtest_name,
      subtest_value,
      subtest_lower_is_better,
      subtest_unit
    ),
    CASE
      rc_subtest_aggregator
      WHEN 'use_existing'
        THEN rc_subtest_value
      WHEN 'mean'
        THEN AVG(rc_subtest_value) OVER (subtest_window)
      WHEN 'median'
        THEN PERCENTILE_CONT(rc_subtest_value, 0.5) OVER (subtest_window)
      WHEN 'geomean'
        -- Adding 1 to the subtest value then subtracting 1 from the result ensures that a value is always returned.
        -- Maintaining here to be consistent with how the perf test framework calculates geomean: https://searchfox.org/mozilla-central/source/testing/talos/talos/filter.py#174
        THEN EXP(
            AVG(LN(IF(rc_subtest_aggregator = 'geomean', rc_subtest_value + 1, NULL))) OVER (
              subtest_window
            )
          ) - 1
      WHEN 'max'
        THEN MAX(rc_subtest_value) OVER (subtest_window)
      WHEN 'min'
        THEN MIN(rc_subtest_value) OVER (subtest_window)
      WHEN 'sum'
        THEN SUM(rc_subtest_value) OVER (subtest_window)
      ELSE ERROR(CONCAT('Unknown subtest aggregator: ', rc_subtest_aggregator))
    END AS rc_test_value,
    AVG(rc_replicate_mean) OVER (subtest_window) AS rc_subtest_mean,
    SQRT(
      SUM(POWER(rc_replicate_stddev, 2)) OVER (subtest_window)
    ) AS rc_propagated_error, -- Propagate error through aggregation
    MAX(rc_replicate_stddev) OVER (subtest_window) AS rc_max_subtest_stddev,
    AVG(rc_replicate_stddev) OVER (subtest_window) AS rc_mean_subtest_stddev,
    COUNT(DISTINCT subtest_name) OVER (subtest_window) AS rc_subtest_count,
--     AVG(rc_replicate_count) OVER(subtest_window) AS rc_subtest_avg_replicates,
    -- Would prefer to just save the array as a JSON string but trying to use `ARRAY_CONCAT_AGG` in the next stage produces an unsupported error
    ARRAY_TO_STRING(
      ARRAY_AGG(CAST(rc_replicate_count AS STRING)) OVER (subtest_window),
      ','
    ) AS rc_subtest_replicate_counts,
    MIN(test_tier) OVER (subtest_window) AS test_tier,
  FROM
    subtests
  WINDOW
    subtest_window AS (
      PARTITION BY
        rc_test_aggregation_key,
        test_name
    )
),
rc_tests AS (
  -- Aggregate the in-scope sub tests and calculate the stddev and count
  SELECT DISTINCT
    * EXCEPT (
      rc_test_value,
      rc_subtest_mean,
      rc_propagated_error,
      rc_max_subtest_stddev,
      rc_mean_subtest_stddev,
      rc_subtest_count,
      rc_subtest_replicate_counts,
      test_name,
      test_value,
      test_tier
    ),
    CASE
      rc_subtest_aggregator
      WHEN 'use_existing'
        THEN rc_test_value
      WHEN 'mean'
        THEN AVG(rc_test_value) OVER (test_window)
      WHEN 'median'
        THEN PERCENTILE_CONT(rc_test_value, 0.5) OVER (test_window)
      WHEN 'geomean'
        -- Adding 1 to the subtest value then subtracting 1 from the result ensures that a value is always returned.
        -- Maintaining here to be consistent with how the perf test framework calculates geomean: https://searchfox.org/mozilla-central/source/testing/talos/talos/filter.py#174
        THEN EXP(
            AVG(LN(IF(rc_test_aggregator = 'geomean', rc_test_value + 1, NULL))) OVER (test_window)
          ) - 1
      WHEN 'max'
        THEN MAX(rc_test_value) OVER (test_window)
      WHEN 'min'
        THEN MIN(rc_test_value) OVER (test_window)
      WHEN 'sum'
        THEN SUM(rc_test_value) OVER (test_window)
      ELSE ERROR(CONCAT('Unknown subtest aggregator: ', rc_subtest_aggregator))
    END AS rc_value,
    AVG(rc_subtest_mean) OVER (test_window) AS rc_test_mean,
    SQRT(
      SUM(POWER(rc_propagated_error, 2)) OVER (test_window)
    ) AS rc_propagated_error, -- Propagate error through aggregation
    MAX(rc_max_subtest_stddev) OVER (test_window) AS rc_max_subtest_stddev,
    AVG(rc_mean_subtest_stddev) OVER (test_window) AS rc_mean_subtest_stddev,
    COUNT(DISTINCT test_name) OVER (test_window) AS rc_test_count,
    AVG(rc_subtest_count) OVER (test_window) AS rc_test_avg_subtests,
    ARRAY_TO_STRING(
      ARRAY_AGG(CAST(rc_subtest_replicate_counts AS STRING)) OVER (test_window),
      ','
    ) AS rc_test_replicate_counts,
    MIN(test_tier) OVER (test_window) AS test_tier,
  FROM
    tests
  WINDOW
    test_window AS (
      PARTITION BY
        rc_test_aggregation_key
    )
),
versions AS (
  SELECT DISTINCT
    date_utc,
    category,
    MAX(`moz-fx-data-bq-performance`.udf.get_version_part(version, 0)) AS major_version,
  FROM
    `dp2-prod`.sumo.release_calendar
  GROUP BY
    date_utc,
    category
),
rc_tests_with_tested_version AS (
  -- Order the columns and reduce to the distinct aggregated tests (quality control on calculations is possible on aggregation temporary tables)
  SELECT DISTINCT
    task_group_time,
    platform,
    normalized_form_factor,
    normalized_device_name,
    normalized_device_os,
    framework,
    project,
    rc_tier,
    recording_date,
    app_name,
    app_version,
    normalized_app_major_version,
    rc_test_name,
    normalized_test_type,
    rc_value,
    rc_propagated_error,
    rc_max_subtest_stddev,
    rc_mean_subtest_stddev,
    rc_test_count,
    rc_test_avg_subtests,
    TO_JSON_STRING(
      (
        SELECT
          APPROX_QUANTILES(CAST(x AS INT64), 3)
        FROM
          UNNEST(SPLIT(rc_test_replicate_counts, ',')) AS x
      )
    ) AS replicate_quantiles,
    rc_target_type,
    rc_target_value,
    rc_target_app,
    test_lower_is_better,
    test_unit,
    IF(
      project IN ('fenix', 'mozilla-central', 'autoland'),
      major_version + 1,
      major_version
    ) AS firefox_version,
    rc_test_aggregation_key,
    task_group_id,
    test_tier
  FROM
    rc_tests
  LEFT JOIN
    versions
    ON date_utc = DATE(task_group_time)
    AND (
      (project IN ('fenix', 'mozilla-central', 'mozilla-beta', 'autoland') AND category = 'dev')
      OR (project = 'mozilla-release' AND category = 'stability')
      OR (project = 'mozilla-esr68' AND category = 'esr')
    )
  WHERE
    rc_value IS NOT NULL
),
build_targets AS (
  -- Builds the results rows for tests with a relative or absolute target value
  SELECT
    * REPLACE (
      CASE
        WHEN rc_target_type = 'absolute'
          THEN 'target'
        WHEN rc_target_type = 'relative'
          AND rc_target_app IS NOT NULL
          THEN CONCAT('target (vs ', rc_target_app, ')')
        WHEN rc_target_type = 'relative'
          AND rc_target_app IS NULL
          THEN ERROR(
              CONCAT(
                'Relative target with null rc_target_app: ',
                platform,
                framework,
                project,
                rc_test_name
              )
            )
      END AS app_name,
      CASE
        WHEN rc_target_type = 'absolute'
          THEN rc_target_value
        WHEN rc_target_type = 'relative'
          THEN rc_target_value * rc_value
      END AS rc_value
    )
  FROM
    rc_tests_with_tested_version
  WHERE
    rc_target_type = 'absolute'
    OR (rc_target_type = 'relative' AND rc_target_app = app_name)
),
rc_tests_with_targets AS (
  SELECT
    *
  FROM
    rc_tests_with_tested_version
  UNION ALL
  SELECT
    *
  FROM
    build_targets
)
SELECT DISTINCT
  *,
  AVG(rc_value) OVER (one_week_window) AS rc_one_week_prior_average,
  SQRT(
    SUM(POWER(rc_propagated_error, 2)) OVER (one_week_window)
  ) AS rc_one_week_prior_propagated_error,
  MAX(rc_max_subtest_stddev) OVER (one_week_window) AS rc_one_week_prior_max_stddev,
  AVG(rc_mean_subtest_stddev) OVER (one_week_window) AS rc_one_week_prior_mean_stddev,
  COUNT(rc_value) OVER (one_week_window) AS rc_one_week_prior_count,
  AVG(rc_value) OVER (two_week_window) AS rc_two_week_prior_average,
  SQRT(
    SUM(POWER(rc_propagated_error, 2)) OVER (two_week_window)
  ) AS rc_two_week_prior_propagated_error,
  MAX(rc_max_subtest_stddev) OVER (two_week_window) AS rc_two_week_prior_max_stddev,
  AVG(rc_mean_subtest_stddev) OVER (two_week_window) AS rc_two_week_prior_mean_stddev,
  COUNT(rc_value) OVER (two_week_window) AS rc_two_week_prior_count,
  AVG(rc_value) OVER (three_week_window) AS rc_three_week_prior_average,
  SQRT(
    SUM(POWER(rc_propagated_error, 2)) OVER (three_week_window)
  ) AS rc_three_week_prior_propagated_error,
  MAX(rc_max_subtest_stddev) OVER (three_week_window) AS rc_three_week_prior_max_stddev,
  AVG(rc_mean_subtest_stddev) OVER (three_week_window) AS rc_three_week_prior_mean_stddev,
  COUNT(rc_value) OVER (three_week_window) AS rc_three_week_prior_count,
  AVG(rc_value) OVER (four_week_window) AS rc_four_week_prior_average,
  SQRT(
    SUM(POWER(rc_propagated_error, 2)) OVER (four_week_window)
  ) AS rc_four_week_prior_propagated_error,
  MAX(rc_max_subtest_stddev) OVER (four_week_window) AS rc_four_week_prior_max_stddev,
  AVG(rc_mean_subtest_stddev) OVER (four_week_window) AS rc_four_week_prior_mean_stddev,
  COUNT(rc_value) OVER (four_week_window) AS rc_four_week_prior_count,
FROM
  rc_tests_with_targets
WINDOW
  moving_average_window AS (
    PARTITION BY
      project,
      platform,
      rc_tier,
      rc_test_name,
      app_name
    ORDER BY
      UNIX_DATE(DATE(task_group_time)) ASC
  ),
  one_week_window AS (
    moving_average_window
    RANGE BETWEEN
      13 PRECEDING
      AND 7 PRECEDING
  ),
  two_week_window AS (
    moving_average_window
    RANGE BETWEEN
      20 PRECEDING
      AND 14 PRECEDING
  ),
  three_week_window AS (
    moving_average_window
    RANGE BETWEEN
      27 PRECEDING
      AND 21 PRECEDING
  ),
  four_week_window AS (
    moving_average_window
    RANGE BETWEEN
      34 PRECEDING
      AND 28 PRECEDING
  )
ORDER BY
  task_group_time DESC,
  rc_test_aggregation_key
