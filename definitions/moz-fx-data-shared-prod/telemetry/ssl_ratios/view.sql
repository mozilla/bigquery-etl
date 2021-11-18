CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.ssl_ratios_v1`
AS
WITH windowed AS (
  SELECT
    *,
    SUM(ssl_loads) OVER w1 + SUM(non_ssl_loads) OVER w1 AS total_loads
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.ssl_ratios_v1`
  WINDOW
    w1 AS (
      ORDER BY
        submission_date
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
)
SELECT
  submission_date,
  os,
  country,
  -- ratio of pings that have the probe
  reporting_ratio,
  -- normalized count of pageloads that went into this ratio
  (non_ssl_loads + ssl_loads) / total_loads AS normalized_pageloads,
  ssl_loads / (non_ssl_loads + ssl_loads) AS ratio
FROM
  windowed
WHERE
  non_ssl_loads + ssl_loads > 5000
