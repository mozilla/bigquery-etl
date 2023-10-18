WITH windowed AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    --
    -- For all dimensions, we use the mode of observed values in the day.
    udf.mode_last(ARRAY_AGG(release_channel) OVER w1) AS release_channel,
    udf.mode_last(ARRAY_AGG(locale) OVER w1) AS locale,
    udf.mode_last(ARRAY_AGG(metadata.geo.country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(version) OVER w1) AS version
  FROM
    firefox_desktop.onboarding
  WHERE
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    (@submission_date IS NULL OR @submission_date = DATE(submission_timestamp))
  WINDOW
    w1 AS (
      PARTITION BY
        client_id,
        DATE(submission_timestamp)
      ORDER BY
        submission_timestamp
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
      PARTITION BY
        client_id,
        DATE(submission_timestamp)
      ORDER BY
        submission_timestamp
    )
)
--
SELECT
  * EXCEPT (_n)
FROM
  windowed
WHERE
  _n = 1
