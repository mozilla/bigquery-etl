CREATE TEMP FUNCTION
  udf_js_jackknife_sum_ci(n_buckets INT64, counts_per_bucket ARRAY<INT64>)
  RETURNS STRUCT<total INT64,
  low INT64,
  high INT64,
  pm INT64>
  LANGUAGE js AS """
function erfinv(x){
    var z;
    var a = 0.147;
    var the_sign_of_x;
    if(0==x) {
        the_sign_of_x = 0;
    } else if(x>0){
        the_sign_of_x = 1;
    } else {
        the_sign_of_x = -1;
    }
    if(0 != x) {
        var ln_1minus_x_sqrd = Math.log(1-x*x);
        var ln_1minusxx_by_a = ln_1minus_x_sqrd / a;
        var ln_1minusxx_by_2 = ln_1minus_x_sqrd / 2;
        var ln_etc_by2_plus2 = ln_1minusxx_by_2 + (2/(Math.PI * a));
        var first_sqrt = Math.sqrt((ln_etc_by2_plus2*ln_etc_by2_plus2)-ln_1minusxx_by_a);
        var second_sqrt = Math.sqrt(first_sqrt - ln_etc_by2_plus2);
        z = second_sqrt * the_sign_of_x;
    } else { // x is zero
        z = 0;
    }
    return z;
}

function jackknife_resamples(arr) {
    output = [];
    for (var i = 0; i < arr.length; i++) {
        var x = arr.slice();
        x.splice(i, 1);
        output.push(x);
    }
    return output;
}

function array_sum(arr) {
    return arr.reduce(function (acc, x) {
        return acc + x;
    }, 0);
}

function array_avg(arr) {
    return array_sum(arr) / arr.length;
}

function sum_buckets_with_ci(n_buckets, counts_per_bucket) {
    if (counts_per_bucket.every(x => x == null)) {
      return null;
    };
    counts_per_bucket = counts_per_bucket.map(x => parseInt(x));
    var total = array_sum(counts_per_bucket);
    var mean = total / n_buckets;
    var resamples = jackknife_resamples(counts_per_bucket);
    var jk_means = resamples.map(x => array_avg(x));
    var mean_errs = jk_means.map(x => Math.pow(x - mean, 2));
    var bucket_std_err = Math.sqrt((n_buckets-1) * array_avg(mean_errs));
    var std_err = n_buckets * bucket_std_err;
    var z_score = Math.sqrt(2.0) * erfinv(0.90);
    var hi = total + z_score * std_err;
    var lo = total - z_score * std_err;
    return {
        "total": total,
        "low": lo,
        "high": hi,
        "pm": hi - total,
    };
}

return sum_buckets_with_ci(n_buckets, counts_per_bucket);
""";
--
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
    `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
  WHERE
    product != 'FirefoxForFireTV' ),
  --
  fxa_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1` ),
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
    udf_js_jackknife_sum_ci(20, ARRAY_AGG(mau)) AS mau_ci,
    udf_js_jackknife_sum_ci(20, ARRAY_AGG(wau)) AS wau_ci,
    udf_js_jackknife_sum_ci(20, ARRAY_AGG(dau)) AS dau_ci
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
