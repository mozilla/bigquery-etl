SELECT
  submission_date,
  "firefox_android" AS application,
  q[1] AS q001,
  q[10] AS q01,
  q[50] AS q05,
  q[500] AS q50,
  q[950] AS q95,
  q[990] AS q99,
  q[999] AS q999
FROM
  (
    SELECT
      DATE(submission_timestamp) AS submission_date,
      APPROX_QUANTILES(CAST(VALUES .key AS INT64), 1000) AS q
    FROM
      `mozdata.fenix.metrics`
    CROSS JOIN
      UNNEST(
        metrics.timing_distribution.places_manager_run_maintenance_chk_pnt_time.values
      ) AS values
  -- This generates multiple rows based on the `value` field.  This is needed to make the `APPROX_QUANTILES`
  -- weigh `value.key` correctly.
    CROSS JOIN
      UNNEST(GENERATE_ARRAY(1, `values`.value))
    GROUP BY
      1
  )
WHERE
  submission_date = @submission_date
