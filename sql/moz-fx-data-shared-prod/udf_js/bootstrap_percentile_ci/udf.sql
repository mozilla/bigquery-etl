/*
Calculates a confidence interval using an efficient bootstrap sampling technique for a
given percentile of a histogram.

See:
- https://mozilla-hub.atlassian.net/browse/DS-2128
- https://engineering.atspotify.com/2022/03/comparing-quantiles-at-scale-in-online-a-b-testing/

Users must specify an array of percentiles of interest as the first parameter and a
histogram struct as the second.
*/

CREATE OR REPLACE FUNCTION udf_js.bootstrap_percentile_ci(
  percentiles ARRAY<INT64>,
  histogram STRUCT<values ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>,
  metric STRING
)
RETURNS ARRAY<STRUCT<
    metric STRING,
    statistic STRING,
    point FLOAT64,
    lower FLOAT64,
    upper FLOAT64,
    parameter STRING
>> DETERMINISTIC
LANGUAGE js OPTIONS ( library=['gs://moz-data-science-bochocki-bucket/bq_udf_library/qbinom.js'])
AS
"""
  function validatePercentile(percentile) {
    if (percentile < 0 || percentile > 100) {
      throw "percentile must be a value between 0 and 100";
    }
  }
  function getPercentile(percentile, quantile_samples) {
    validatePercentile(percentile);
    const size = quantile_samples.length;
    if (size == 0) {
      return null;
    }

    /* get (0-indexed) location of the target percentile */
    var index = Math.floor(size * parseFloat(percentile) / 100) - 1;
    return quantile_samples[index];
  }

  function numericSort(arr) {
    arr.sort(function(a, b) {return a - b;});
  }
  
  function getQuantileSamples(percentile, histogram, n_samples) {
    validatePercentile(percentile);

    /* expand the histogram */
    var full_histogram = histogram.map(bin => Array(bin.value).fill(parseFloat(bin.key))).reduce((prev, curr) => prev.concat(curr));
    numericSort(full_histogram);

    /* sample the quantile of interest */
    var output = [];
    var index = null;
    const size = full_histogram.length - 1;
    for (var i = 0; i < n_samples; i++) {
      index = qbinom(Math.random(), size, parseFloat(percentile) / 100);
      output.push(full_histogram[index]);
    }

    numericSort(output);
    return output;
  }

  function getPercentilesWithCI(percentiles, histogram, metric) {
    var results = [];
    
    if (!percentiles.length) {
      throw "percentiles must be a non-empty array of integers";
    }

    if (histogram.values.some(bin => isNaN(bin.key) || isNaN(bin.value) || (bin.key === null) || (bin.value === null))) {
      throw "histogram can only contain non-null numeric values";
    }

    for (var i = 0; i < percentiles.length; i++) {
      percentile = percentiles[i];
      var lower = null;
      var upper = null;
      var point = null;
      
      if (!histogram.values || !histogram ) {
        continue;
      } else {
        var quantile_samples = getQuantileSamples(percentile, histogram.values, 1000);
        lower = parseFloat(getPercentile(5, quantile_samples)).toFixed(2);
        point = parseFloat(getPercentile(50, quantile_samples)).toFixed(2);
        upper = parseFloat(getPercentile(95, quantile_samples)).toFixed(2);
      }
        
      results.push({
        "metric": metric,
        "statistic": "percentile",
        "lower": lower,
        "point": point,
        "upper": upper,
        "parameter": percentile,
      });
    }
    return results;
  }
  return getPercentilesWithCI(percentiles, histogram, metric);
  """;

-- Tests
WITH test_data AS (
  SELECT 'test' AS metric_name,

  STRUCT([
    STRUCT(0.0 AS key,  0.0 AS value),
    STRUCT(1.0 AS key, NULL AS value),
    STRUCT(2.0 AS key,  0.0 AS value)
  ] AS values) AS null_value,

  STRUCT([
    STRUCT( 0.0 AS key,  0.0 AS value),
    STRUCT(NULL AS key, 10.0 AS value),
    STRUCT( 2.0 AS key,  0.0 AS value)
  ] AS values) AS null_key,

  STRUCT([
    STRUCT( 0.0 AS key,  0.0 AS value),
    STRUCT( 1.0 AS key, 10.0 AS value),
    STRUCT( 2.0 AS key,  0.0 AS value)
  ] AS values) AS one_bin,

  STRUCT([
    STRUCT(-5.0 AS key,  0.0 AS value),
    STRUCT(-4.0 AS key, 10.0 AS value),
    STRUCT(-3.0 AS key, 10.0 AS value),
    STRUCT(-2.0 AS key, 10.0 AS value),
    STRUCT(-1.0 AS key, 10.0 AS value),
    STRUCT( 0.0 AS key, 10.0 AS value),
    STRUCT( 1.0 AS key, 10.0 AS value),
    STRUCT( 2.0 AS key, 10.0 AS value),
    STRUCT( 3.0 AS key, 10.0 AS value),
    STRUCT( 4.0 AS key, 10.0 AS value),
    STRUCT( 5.0 AS key,  0.0 AS value)
  ] AS values) AS nine_bin
),

one_bin_percentiles AS (
  SELECT udf_js.bootstrap_percentile_ci([5, 10, 50, 90, 95], one_bin, metric_name) AS output FROM test_data
),

one_bin_results AS (
  SELECT output.* FROM one_bin_percentiles, UNNEST(output) AS output
),

nine_bin_percentiles AS (
  SELECT udf_js.bootstrap_percentile_ci([5, 10, 50, 90, 95], nine_bin, metric_name) AS output FROM test_data
),

nine_bin_results AS (
  SELECT output.* FROM nine_bin_percentiles, UNNEST(output) AS output
)

-- valid outputs
SELECT
assert.equals('test', (SELECT DISTINCT metric FROM one_bin_results)),
assert.equals('percentile', (SELECT DISTINCT statistic FROM one_bin_results)),
assert.array_equals((SELECT ['5', '10', '50', '90', '95']), (SELECT ARRAY(SELECT parameter FROM one_bin_results))),
assert.array_equals((SELECT [1.0, 1.0, 1.0, 1.0, 1.0]), (SELECT ARRAY(SELECT lower FROM one_bin_results))),
assert.array_equals((SELECT [1.0, 1.0, 1.0, 1.0, 1.0]), (SELECT ARRAY(SELECT point FROM one_bin_results))),
assert.array_equals((SELECT [1.0, 1.0, 1.0, 1.0, 1.0]), (SELECT ARRAY(SELECT upper FROM one_bin_results))),

assert.equals('test', (SELECT DISTINCT metric FROM nine_bin_results)),
assert.equals('percentile', (SELECT DISTINCT statistic FROM nine_bin_results)),
assert.array_equals((SELECT ['5', '10', '50', '90', '95']), (SELECT ARRAY(SELECT parameter FROM nine_bin_results))),
assert.array_equals((SELECT [-4.0, -4.0, -1.0, 3.0, 4.0]), (SELECT ARRAY(SELECT lower FROM nine_bin_results))),
assert.array_equals((SELECT [-4.0, -4.0,  0.0, 4.0, 4.0]), (SELECT ARRAY(SELECT point FROM nine_bin_results))),
assert.array_equals((SELECT [-4.0, -3.0,  1.0, 4.0, 4.0]), (SELECT ARRAY(SELECT upper FROM nine_bin_results)));

-- bad percentile arguments
#xfail
SELECT udf_js.bootstrap_percentile_ci(   [], one_bin_histogram, metric_name) FROM test_data;
#xfail
SELECT udf_js.bootstrap_percentile_ci([-10], one_bin_histogram, metric_name) FROM test_data;
#xfail
SELECT udf_js.bootstrap_percentile_ci([110], one_bin_histogram, metric_name) FROM test_data;

-- malformed histograms
#xfail
SELECT udf_js.bootstrap_percentile_ci([50], null_value_histogram, metric_name) FROM test_data;
#xfail
SELECT udf_js.bootstrap_percentile_ci([50], null_key_histogram, metric_name) FROM test_data;
