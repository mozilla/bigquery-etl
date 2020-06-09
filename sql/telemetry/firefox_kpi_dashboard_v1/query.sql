WITH
  forecast_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.analysis.growth_dashboard_forecasts_current`
  -- We patch in nondesktop forecasts that exclude Firefox for Fire TV.
  WHERE
    datasource NOT IN ('nondesktop_global', 'nondesktop_tier1')
  UNION ALL
  SELECT
    * REPLACE (REPLACE(datasource, '_nofire', '') AS datasource)
  FROM
    `moz-fx-data-derived-datasets.analysis.growth_dashboard_forecasts_nofire`
  ),
  --
  desktop_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.telemetry.firefox_desktop_exact_mau28_by_dimensions_v1` ),
  --
  nondesktop_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
  WHERE
    product != 'FirefoxForFireTV' ),
  --
  fxa_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1` ),
  --
  --
  per_bucket AS (
  SELECT
    'desktop_global' AS datasource,
    'actual' AS type,
    submission_date,
    id_bucket,
    SUM(mau) AS mau,
    SUM(wau) AS wau,
    SUM(dau) AS dau
  FROM
    desktop_base
  GROUP BY
    id_bucket,
    submission_date
  UNION ALL
  SELECT
    'desktop_tier1' AS datasource,
    'actual' AS type,
    submission_date AS date,
    id_bucket,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS mau,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), wau, 0)) AS wau,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), dau, 0)) AS dau
  FROM
    desktop_base
  GROUP BY
    id_bucket,
    submission_date
    --
  UNION ALL
    --
  SELECT
    'nondesktop_global' AS datasource,
    'actual' AS type,
    submission_date,
    id_bucket,
    SUM(mau) AS mau,
    SUM(wau) AS wau,
    SUM(dau) AS dau
  FROM
    nondesktop_base
  GROUP BY
    id_bucket,
    submission_date
  UNION ALL
  SELECT
    'nondesktop_tier1' AS datasource,
    'actual' AS type,
    submission_date AS date,
    id_bucket,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS mau,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), wau, 0)) AS wau,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), dau, 0)) AS dau
  FROM
    nondesktop_base
  GROUP BY
    id_bucket,
    submission_date
    --
  UNION ALL
    --
  SELECT
    'fxa_global' AS datasource,
    'actual' AS type,
    submission_date,
    id_bucket,
    SUM(mau) AS mau,
    SUM(wau) AS wau,
    SUM(dau) AS dau
  FROM
    fxa_base
  GROUP BY
    id_bucket,
    submission_date
  UNION ALL
  SELECT
    'fxa_tier1' AS datasource,
    'actual' AS type,
    submission_date AS date,
    id_bucket,
    -- We have a special way of determining whether to include a user in tier1
    -- to match historical definition of tier1 FxA mau.
    SUM(seen_in_tier1_country_mau) AS mau,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), wau, 0)) AS wau,
    SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), dau, 0)) AS dau
  FROM
    fxa_base
  GROUP BY
    id_bucket,
    submission_date ),
  --
  --
  with_ci AS (
  SELECT
    datasource,
    type,
    submission_date,
    udf_js.jackknife_sum_ci(20, ARRAY_AGG(mau)) AS mau_ci,
    udf_js.jackknife_sum_ci(20, ARRAY_AGG(wau)) AS wau_ci,
    udf_js.jackknife_sum_ci(20, ARRAY_AGG(dau)) AS dau_ci
  FROM
    per_bucket
  GROUP BY
    datasource,
    type,
    submission_date ),
  --
  --
  with_forecast AS (
  SELECT
    datasource,
    type,
    submission_date AS `date`,
    mau_ci.total AS mau,
    mau_ci.low AS mau_low,
    mau_ci.high AS mau_high,
    wau_ci.total AS wau,
    wau_ci.low AS wau_low,
    wau_ci.high AS wau_high,
    dau_ci.total AS dau,
    dau_ci.low AS dau_low,
    dau_ci.high AS dau_high
  FROM
    with_ci
  UNION ALL
  SELECT
    datasource,
    type,
    `date`,
    mau,
    low90 AS mau_low,
    high90 AS mau_high,
    -- We only have forecasts for MAU, so we use null for forecast DAU and WAU.
    null as wau,
    null as wau_low,
    null as wau_high,
    null as dau,
    null as dau_low,
    null as dau_high
  FROM
    forecast_base
  WHERE
    type != 'original'
    -- Make sure we don't accidentally have the actuals and forecast overlap
    AND `date` > (SELECT MAX(submission_date) FROM with_ci) ),
  -- We use imputed values for the period after the Armag-add-on deletion event;
  -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1552558
  with_imputed AS (
  SELECT
    with_forecast.* REPLACE (
      COALESCE(imputed.mau, with_forecast.mau) AS mau,
      COALESCE(imputed.mau + imputed.mau * 0.005, with_forecast.mau_high) AS mau_high,
      COALESCE(imputed.mau - imputed.mau * 0.005, with_forecast.mau_low) AS mau_low )
  FROM
    with_forecast
  LEFT JOIN
    static.firefox_desktop_imputed_mau28_v1 AS imputed
  USING
    (datasource, `date`)
  )
  --
SELECT
  *
FROM
  with_imputed
ORDER BY
  datasource,
  type,
  `date`
