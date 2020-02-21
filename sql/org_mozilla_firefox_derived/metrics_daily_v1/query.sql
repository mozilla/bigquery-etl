/*

Very similar to baseline_daily, but considers only metrics pings.

*/
CREATE TEMP FUNCTION extract_fields(metrics ANY TYPE) AS (
  (
    SELECT AS STRUCT
      metrics.submission_timestamp,
      DATE(metrics.submission_timestamp) AS submission_date,
      LOWER(metrics.client_info.client_id) AS client_id,
      metrics.sample_id,
      metrics.metrics.string.search_default_engine_name,
      metrics.metrics.string.search_default_engine_code,
      metrics.metrics.boolean.metrics_default_browser,
  )
);

WITH base AS (
  SELECT
    extract_fields(metrics).*,
  FROM
    org_mozilla_firefox.metrics AS metrics
  UNION ALL
  SELECT
    extract_fields(metrics).*,
  FROM
    org_mozilla_firefox_beta.metrics AS metrics
  UNION ALL
  SELECT
    extract_fields(metrics).*,
  FROM
    org_mozilla_fennec_aurora.metrics AS metrics
  UNION ALL
  SELECT
    extract_fields(metrics).*,
  FROM
    org_mozilla_fenix.metrics AS metrics
  UNION ALL
  SELECT
    extract_fields(metrics).*,
  FROM
    org_mozilla_fenix_nightly.metrics AS metrics
),
--
windowed AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    ROW_NUMBER() OVER w1_unframed AS _n,
    --
    -- For all other dimensions, we use the mode of observed values in the day.
    udf.mode_last(ARRAY_AGG(search_default_engine_name) OVER w1) AS search_default_engine_name,
    udf.mode_last(ARRAY_AGG(search_default_engine_code) OVER w1) AS search_default_engine_code,
    udf.mode_last(ARRAY_AGG(metrics_default_browser) OVER w1) AS metrics_default_browser,
  FROM
    base
  WHERE
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    (@submission_date IS NULL OR @submission_date = submission_date)
  WINDOW
    w1 AS (
      PARTITION BY
        sample_id,
        client_id,
        submission_date
      ORDER BY
        submission_timestamp
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ),
    -- We must provide a modified window for ROW_NUMBER which cannot accept a frame clause.
    w1_unframed AS (
      PARTITION BY
        sample_id,
        client_id,
        submission_date
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
