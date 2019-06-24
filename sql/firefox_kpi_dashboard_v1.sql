CREATE TEMP FUNCTION
  udf_sum_buckets_with_ci(a ARRAY<INT64>)
  RETURNS STRUCT<total INT64,
  low INT64,
  high INT64,
  pm INT64,
  nbuckets INT64>
  LANGUAGE js AS """
function erfinv(x){
    var z;
    var a  = 0.147;
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
function sum_buckets_with_ci(counts_per_bucket) {
    counts_per_bucket = counts_per_bucket.map(x => parseInt(x));
    var n = counts_per_bucket.length;
    var total = array_sum(counts_per_bucket);
    var mean = total / n;
    var resamples = jackknife_resamples(counts_per_bucket);
    var jk_means = resamples.map(x => array_avg(x));
    var mean_errs = jk_means.map(x => Math.pow(x - mean, 2));
    var bucket_std_err = Math.sqrt((n-1) * array_avg(mean_errs));
    var std_err = n * bucket_std_err;
    var z_score = Math.sqrt(2.0) * erfinv(0.90);
    var hi = total + z_score * std_err;
    var lo = total - z_score * std_err;
    return {
        "total": total,
        "low": lo,
        "high": hi,
        "pm": hi - total,
        "nbuckets": n,
    };
}
return sum_buckets_with_ci(a);
""";
--
WITH
  forecast_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.analysis.growth_dashboard_forecasts_current` ),
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
    `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1` ),
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
    SUM(mau) AS mau
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
    SUM(
    IF
      (country IN ('US',
          'FR',
          'DE',
          'GB',
          'CA'),
        mau,
        0)) AS mau
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
    SUM(mau) AS mau
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
    SUM(
    IF
      (country IN ('US',
          'FR',
          'DE',
          'GB',
          'CA'),
        mau,
        0)) AS mau
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
    SUM(mau) AS mau
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
    SUM(seen_in_tier1_country_mau) AS mau
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
    udf_sum_buckets_with_ci(ARRAY_AGG(mau)).*
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
    total AS mau,
    low AS mau_low,
    high AS mau_high
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
    type != 'original'),
  -- We use imputed values for the period after the Armag-add-on deletion event;
  -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1552558
  with_imputed AS (
  SELECT
    *
  FROM
    with_forecast
  WHERE
    NOT (`date` BETWEEN '2019-05-15'
      AND '2019-06-08'
      AND datasource IN ('desktop_global',
        'desktop_tier1'))
  UNION ALL
  SELECT
    datasource,
    'actual' AS type,
    `date`,
    mau,
    -- The confidence interval is chosen here somewhat arbitrarily as 0.5% of
    -- the value; in any case, we should show a larger interval than we do for
    -- non-imputed actuals.
    mau - mau * 0.005 AS mau_low,
    mau + mau * 0.005 AS mau_high
  FROM
    static.firefox_desktop_imputed_mau28_v1 )
  --
SELECT
  *
FROM
  with_imputed
ORDER BY
  datasource,
  type,
  `date`
