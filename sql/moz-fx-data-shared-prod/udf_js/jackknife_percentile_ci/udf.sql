/*

Calculates a confidence interval using a jackknife resampling technique
for a given percentile of a histogram. See
https://en.wikipedia.org/wiki/Jackknife_resampling

Users must specify the percentile of interest as the first parameter
and a histogram struct as the second.

*/
CREATE OR REPLACE FUNCTION udf_js.jackknife_percentile_ci(
  percentile FLOAT64,
  histogram STRUCT<values ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>
)
RETURNS STRUCT<low FLOAT64, high FLOAT64, percentile FLOAT64> DETERMINISTIC
LANGUAGE js
AS
"""
  function computePercentile(percentile, histogram) {
    if (percentile < 0 || percentile > 100) {
      throw "percentile must be a value between 0 and 100";
    }
    let values = histogram.map(bucket => parseFloat(bucket.value));
    let total = values.reduce((a, b) => a + b, 0);
    let normalized = values.map(value => value / total);
    // Find the index into the cumulative distribution function that corresponds
    // to the percentile. This undershoots the true value of the percentile.
    let acc = 0;
    let index = null;
    for (let i = 0; i < normalized.length; i++) {
      acc += normalized[i];
      index = i;
      if (acc >= percentile / 100) {
        break;
      }
    }
    // NOTE: we do not perform geometric or linear interpolation, but this would
    // be the place to implement it.
    if (histogram.length == 0) {
      return null;
    }

    return histogram[index].key;
  }

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

  function array_sum(arr) {
      return arr.reduce(function (acc, x) {
          return acc + x;
      }, 0);
  }

  function array_avg(arr) {
      return array_sum(arr) / arr.length;
  }

  function getMeanErrorsPercentile(percentile, histogram, fullDataPercentile) {
    var jk_percentiles = [];
    histogram.values.forEach((bucket, i) => {
      var histCopy = JSON.parse(JSON.stringify(histogram.values));
      histCopy[i].value--;
      jk_percentiles.push(computePercentile(percentile, histCopy));
    });
    return jk_percentiles.map(x => Math.pow(x - fullDataPercentile, 2));
  }

  function percentile_with_ci(percentile, histogram) {
    var fullDataPercentile = parseFloat(computePercentile(percentile, histogram.values));
    var meanErrors = getMeanErrorsPercentile(percentile, histogram, fullDataPercentile);
    var count = histogram.values.reduce((acc, curr) => acc + parseFloat(curr.value), 0);
    var std_err = Math.sqrt((count - 1) * array_avg(meanErrors));
    var z_score = Math.sqrt(2.0) * erfinv(0.90);
    var hi = fullDataPercentile + (z_score * std_err);
    var lo = fullDataPercentile - (z_score * std_err);
    return {
      "low": lo.toFixed(2),
      "high": hi.toFixed(2),
      "percentile": fullDataPercentile
    };
  }

  return percentile_with_ci(percentile, histogram);
  """;

-- Tests
WITH jackknife AS (
  SELECT
    udf_js.jackknife_percentile_ci(
      10.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_10,
    udf_js.jackknife_percentile_ci(
      20.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_20,
    udf_js.jackknife_percentile_ci(
      30.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_30,
    udf_js.jackknife_percentile_ci(
      40.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_40,
    udf_js.jackknife_percentile_ci(
      50.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_50,
    udf_js.jackknife_percentile_ci(
      60.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_60,
    udf_js.jackknife_percentile_ci(
      70.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_70,
    udf_js.jackknife_percentile_ci(
      80.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_80,
    udf_js.jackknife_percentile_ci(
      90.0,
      STRUCT(
        [
          STRUCT(1 AS key, 3.0 AS value),
          STRUCT(2.0, 2),
          STRUCT(4, 1),
          STRUCT(5, 1),
          STRUCT(6, 2)
        ] AS values
      )
    ) AS percentile_90
)
SELECT
  mozfun.assert.equals(1, percentile_10.high),
  mozfun.assert.equals(1, percentile_10.low),
  mozfun.assert.equals(1, percentile_10.percentile),
  mozfun.assert.equals(1, percentile_20.high),
  mozfun.assert.equals(1, percentile_20.low),
  mozfun.assert.equals(1, percentile_20.percentile),
  mozfun.assert.equals(3.08, percentile_30.high),
  mozfun.assert.equals(-1.08, percentile_30.low),
  mozfun.assert.equals(1, percentile_30.percentile),
  mozfun.assert.equals(2, percentile_40.high),
  mozfun.assert.equals(2, percentile_40.low),
  mozfun.assert.equals(2, percentile_40.percentile),
  mozfun.assert.equals(2, percentile_50.high),
  mozfun.assert.equals(2, percentile_50.low),
  mozfun.assert.equals(2, percentile_50.percentile),
  mozfun.assert.equals(11.21, percentile_60.high),
  mozfun.assert.equals(-3.21, percentile_60.low),
  mozfun.assert.equals(4, percentile_60.percentile),
  mozfun.assert.equals(7.94, percentile_70.high),
  mozfun.assert.equals(2.06, percentile_70.low),
  mozfun.assert.equals(5, percentile_70.percentile),
  mozfun.assert.equals(8.08, percentile_80.high),
  mozfun.assert.equals(3.92, percentile_80.low),
  mozfun.assert.equals(6, percentile_80.percentile),
  mozfun.assert.equals(6, percentile_90.high),
  mozfun.assert.equals(6, percentile_90.low),
  mozfun.assert.equals(6, percentile_90.percentile)
FROM
  jackknife
