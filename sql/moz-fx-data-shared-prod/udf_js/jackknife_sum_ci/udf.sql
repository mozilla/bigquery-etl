/*

Calculates a confidence interval using a jackknife resampling technique
for the sum of an array of counts for various buckets; see
https://en.wikipedia.org/wiki/Jackknife_resampling

Users must specify the number of expected buckets as the first parameter
to guard against the case where empty buckets lead to an array with missing
elements.

Usage generally involves first calculating an aggregate count per bucket,
then aggregating over buckets, passing ARRAY_AGG(metric) to this function.

Example:

WITH bucketed AS (
  SELECT
    submission_date,
    SUM(dau) AS dau_sum
  FROM
    mytable
  GROUP BY
    submission_date,
    bucket_id )
SELECT
  submission_date,
  udf_js.jackknife_sum_ci(ARRAY_AGG(dau_sum)).*
FROM
  bucketed
GROUP BY
  submission_date

*/
CREATE OR REPLACE FUNCTION udf_js.jackknife_sum_ci(n_buckets INT64, counts_per_bucket ARRAY<INT64>)
RETURNS STRUCT<total INT64, low INT64, high INT64, pm INT64> DETERMINISTIC
LANGUAGE js
AS
  """
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

function pad_array(arr, len, fill) {
    return arr.concat(Array(len).fill(fill)).slice(0,len);
}

function sum_buckets_with_ci(n_buckets, counts_per_bucket) {
    if (counts_per_bucket.every(x => x == null)) {
      return null;
    };
    counts_per_bucket = pad_array(counts_per_bucket, n_buckets, 0);
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

-- Tests
SELECT
  -- Make sure a single-element array doesn't throw an error.
  mozfun.assert.equals(5, udf_js.jackknife_sum_ci(20, [5]).total)
