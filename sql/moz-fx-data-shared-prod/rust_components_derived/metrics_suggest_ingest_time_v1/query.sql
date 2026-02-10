SELECT
  submission_date,
  "firefox_desktop" AS application,
  channel,
  label,
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
      normalized_channel AS channel,
      distribution.key AS label,
      APPROX_QUANTILES(CAST(VALUES .key AS INT64), 1000) AS q
    FROM
      `mozdata.firefox_desktop.metrics`
    CROSS JOIN
      UNNEST(metrics.labeled_timing_distribution.suggest_ingest_time) AS distribution
    CROSS JOIN
      UNNEST(distribution.value.values) AS values
  -- This generates multiple rows based on the `value` field.  This is needed to make the `APPROX_QUANTILES`
  -- weigh `value.key` correctly.
    CROSS JOIN
      UNNEST(GENERATE_ARRAY(1, `values`.value))
    GROUP BY
      1,
      2,
      3
  )
WHERE
  submission_date = @submission_date
