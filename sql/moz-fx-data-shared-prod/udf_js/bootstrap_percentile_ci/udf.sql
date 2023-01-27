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
LANGUAGE js OPTIONS ( library=['gs://moz-fx-data-circleci-tests-bigquery-etl/bq_udf_library/qbinom.js'])
AS
"""
  function histogramSort(histogram) {
    histogram.sort(function(a, b) {return parseFloat(a.key) - parseFloat(b.key);});
  }

  function validatePercentile(percentile) {
    if (percentile < 0 || percentile > 100) {
      throw "percentile must be a value between 0 and 100";
    }
  }
  
  function getQuantileSamples(percentile, histogram, n_samples) {
    /*
    Returns a sorted array of quantile samples from a `histogram` at a target `percentile`.
    */
    validatePercentile(percentile);

    /* ensure the histogram is sorted */
    histogramSort(histogram);

    /* calculate the cumulative number of elements in the histogram */
    var count = 0;
    histogram.forEach(function (bin, i) {
        count += histogram[i].value;
        histogram[i].count = count;
    });

    var samples = [];
    /* sample the quantile of interest */
    for (var i = 0; i < n_samples; i++) {
      var sample = qbinom(Math.random(), count, parseFloat(percentile) / 100);
      var target = Math.min(sample, count);

      if (target in samples) {
        samples[target].value += 1;
      } else {
        for (const x of histogram) {
          if (x.count >= target) {
            samples[target] = {'key': x.key, 'value': 1};
            break;
          }
        };
      }
    }

    /* ensure the histogram is sorted */
    histogramSort(samples);

    /* expand the output */
    var output = samples.map(bin => Array(bin.value).fill(parseFloat(bin.key))).reduce((prev, curr) => prev.concat(curr));

    return output;
  }

  function getPercentileBin(percentile, quantile_samples) {
    /* 
    Returns the target `percentile` from a sorted array of samples.
    */
    validatePercentile(percentile);
    const size = quantile_samples.length;
    if (size == 0) {
      return null;
    }

    /* get (0-indexed) location of the target percentile */
    var index = Math.floor(size * parseFloat(percentile) / 100) - 1;
    return quantile_samples[index];
  }

  function binToValue(bin_value, histogram, location) {
    /* 
    Returns the estimated value of a `location` inside a histogram bin,
    where `location` is between 0 (left edge) and 1 (right edge).
    */
    if (location < 0 || location > 1) {
      throw "location must be a value between 0 and 1";
    }

    const size = histogram.length;
    if (size == 0) {
      return null;
    }

    var index = null;
    histogram.forEach(function (bin, i) {
      if (bin.key === bin_value) {
        index = i;
      }
    });

    left = parseFloat(histogram[index].key);
    right = parseFloat(histogram[Math.min(index+1, size-1)].key);

    return (left + ((right-left)*location)).toFixed(2);
  }

  function getPercentilesWithCI(percentiles, histogram, metric) {
    
    if (!percentiles.length) {
      throw "percentiles must be a non-empty array of integers";
    }

    if (histogram.values.some(bin => isNaN(bin.key) || isNaN(bin.value) || (bin.key === null) || (bin.value === null))) {
      throw "histogram can only contain non-null numeric values";
    }

    var results = [];
    for (var i = 0; i < percentiles.length; i++) {
      percentile = percentiles[i];
      var lower = null;  /* left edge of lower estimate bin */
      var point = null;  /* center of point estimate bin */
      var upper = null;  /* right edge of upper estimate bin */
      
      if (!histogram.values || !histogram ) {
        continue;
      } else {
        var quantile_samples = getQuantileSamples(percentile, histogram.values, 10000);
        lower = binToValue(getPercentileBin( 5, quantile_samples), histogram.values, 0.0);
        point = binToValue(getPercentileBin(50, quantile_samples), histogram.values, 0.5);
        upper = binToValue(getPercentileBin(95, quantile_samples), histogram.values, 1.0);
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
    STRUCT(-4.5 AS key,  0.0 AS value),
    STRUCT(-3.5 AS key,  1.0 AS value),
    STRUCT(-2.5 AS key, 12.0 AS value),
    STRUCT(-1.5 AS key, 22.0 AS value),
    STRUCT(-0.5 AS key, 30.0 AS value),
    STRUCT( 0.5 AS key, 22.0 AS value),
    STRUCT( 1.5 AS key, 12.0 AS value),
    STRUCT( 2.5 AS key,  1.0 AS value),
    STRUCT( 3.5 AS key,  0.0 AS value)
  ] AS values) AS seven_bin
),

one_bin_percentiles AS (
  SELECT udf_js.bootstrap_percentile_ci([5, 25, 50, 75, 95], one_bin, metric_name) AS output FROM test_data
),

one_bin_results AS (
  SELECT output.* FROM one_bin_percentiles, UNNEST(output) AS output
),

seven_bin_percentiles AS (
  SELECT udf_js.bootstrap_percentile_ci([5, 25, 50, 75, 95], seven_bin, metric_name) AS output FROM test_data
),

seven_bin_results AS (
  SELECT output.* FROM seven_bin_percentiles, UNNEST(output) AS output
)

-- valid outputs
SELECT
assert.equals('test', (SELECT DISTINCT metric FROM one_bin_results)),
assert.equals('percentile', (SELECT DISTINCT statistic FROM one_bin_results)),
assert.array_equals((SELECT ['5', '25', '50', '75', '95']), (SELECT ARRAY(SELECT parameter FROM one_bin_results))),
assert.array_equals((SELECT [1.0, 1.0, 1.0, 1.0, 1.0]), (SELECT ARRAY(SELECT lower FROM one_bin_results))),
assert.array_equals((SELECT [1.5, 1.5, 1.5, 1.5, 1.5]), (SELECT ARRAY(SELECT point FROM one_bin_results))),
assert.array_equals((SELECT [2.0, 2.0, 2.0, 2.0, 2.0]), (SELECT ARRAY(SELECT upper FROM one_bin_results))),

assert.equals('test', (SELECT DISTINCT metric FROM seven_bin_results)),
assert.equals('percentile', (SELECT DISTINCT statistic FROM seven_bin_results)),
assert.array_equals((SELECT ['5', '25', '50', '75', '95']), (SELECT ARRAY(SELECT parameter FROM seven_bin_results))),
assert.array_equals((SELECT [-2.5, -1.5, -0.5, 0.5, 1.5]), (SELECT ARRAY(SELECT lower FROM seven_bin_results))),
assert.array_equals((SELECT [-2.0, -1.0,  0.0, 1.0, 2.0]), (SELECT ARRAY(SELECT point FROM seven_bin_results))),
assert.array_equals((SELECT [-1.5, -0.5,  0.5, 1.5, 2.5]), (SELECT ARRAY(SELECT upper FROM seven_bin_results)));

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
