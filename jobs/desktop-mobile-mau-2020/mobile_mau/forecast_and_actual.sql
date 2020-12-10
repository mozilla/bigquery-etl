WITH forecast_base AS (
  SELECT
    REPLACE(datasource, " Global MAU", "") AS datasource,
    date,
    type,
    value AS mau,
    low90,
    high90,
    p10 AS low80,
    p90 AS high80,
    p20,
    p30,
    p40,
    p50,
    p60,
    p70,
    p80
  FROM
    `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`
  WHERE
    asofdate = (
      SELECT
        MAX(asofdate)
      FROM
        `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`
    )
    AND datasource IN (
      "Fenix Global MAU",
      "Fennec Global MAU",
      "Firefox iOS Global MAU",
      "Firefox Lite Global MAU",
      "Firefox Echo Global MAU",
      "Focus Android Global MAU",
      "Focus iOS Global MAU",
      "Lockwise Android Global MAU"
    )
),
mobile_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
  WHERE
    product IN (
      "Fenix",
      "Fennec",
      "Firefox iOS",
      "Firefox Lite",
      "Firefox Echo",
      "Focus Android",
      "Focus iOS",
      "Lockwise Android"
    )
),
per_bucket AS (
  SELECT
    product AS datasource,
    'actual' AS type,
    submission_date,
    id_bucket,
    SUM(mau) AS mau
  FROM
    mobile_base
  GROUP BY
    product,
    id_bucket,
    submission_date
),
with_ci AS (
  SELECT
    datasource,
    type,
    submission_date,
    `moz-fx-data-shared-prod.udf_js.jackknife_sum_ci`(20, ARRAY_AGG(mau)) AS mau
  FROM
    per_bucket
  GROUP BY
    datasource,
    type,
    submission_date
),
with_forecast AS (
  SELECT
    datasource,
    type,
    submission_date AS `date`,
    mau.total AS mau,
    mau.low AS mau_low,
    mau.high AS mau_high
  FROM
    with_ci
  UNION ALL
  SELECT
    datasource,
    type,
    `date`,
    mau,
    low90 AS mau_low,
    high90 AS mau_high
  FROM
    forecast_base
  WHERE
    type != 'original'
    AND date > (SELECT MAX(submission_date) FROM with_ci)
)
SELECT
  datasource,
  type,
  `date`,
  mau,
  mau_low,
  mau_high
FROM
  with_forecast
ORDER BY
  datasource,
  type,
  `date`
