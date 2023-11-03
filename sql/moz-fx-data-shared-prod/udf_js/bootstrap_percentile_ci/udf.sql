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
RETURNS ARRAY<
  STRUCT<
    metric STRING,
    statistic STRING,
    point FLOAT64,
    lower FLOAT64,
    upper FLOAT64,
    parameter STRING
  >
> DETERMINISTIC
LANGUAGE js
OPTIONS
  (library = "gs://moz-fx-data-circleci-tests-bigquery-etl/qbinom.js")
AS
  """
  function histogramSort(histogram) {
    histogram.sort(function(a, b) {return parseFloat(a.key) - parseFloat(b.key);});
  }

  function arraySort(x) {
    x.sort(function(a, b) {return parseFloat(a) - parseFloat(b);});
  }

  function histogramIsNonZero(histogram) {
    return histogram.some(bin => parseFloat(bin.value) > 0.0);
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
    var total = 0;
    for (var bin of histogram) {
        if ( bin.value > 0 ) {
          /* include this non-empty bin when drawing samples */
          total += bin.value;
          bin.cumulative_sum = total;
        } else {
          /* ignore this empty bin when drawing samples */
          bin.cumulative_sum = -1;
        }
    };

    /*
    Use a `scale` to approximate non-integer samples from qbinom. This is required because
    histogram values may not always be counts; if the histogram is normalized, bins can
    contain floats.
    */
    const scale = 1000;
    total = Math.ceil(total * scale);

    /* sample the quantile of interest */
    var samples = [];
    for ( var i = 0; i < n_samples; i++ ) {
      /* draw a random variable from the binomial distribution */
      var x = qbinom(Math.random(), total, parseFloat(percentile) / 100) / scale;
      if ( x in samples ) {
        samples[x].value += 1;
      } else {
        /* find the histogram bin associated with the sample */
        for ( const bin of histogram ) {
          if ( bin.cumulative_sum >= x ) {
            samples[x] = {'key': bin.key, 'value': 1};
            break;
          }
        };
      }
    }

    /* build the output from the sorted sample keys */
    sample_keys = Object.keys(samples);
    arraySort(sample_keys);
    output = [];
    for ( const x of sample_keys ) {
        const bin = samples[x];
        for(var i = 0; i < bin.value; i++){
            output.push(bin.key);
        }
    }

    return output;
  }

  function getPercentileBin(percentile, quantile_samples) {
    /*
    Returns the target `percentile` from a sorted array of samples.
    */
    validatePercentile(percentile);
    const size = quantile_samples.length;
    if ( size == 0 ) {
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
    histogram.forEach( function (bin, i) {
      if (bin.key === bin_value) {
        index = i;
      }
    });

    left = parseFloat(histogram[index].key);
    right = parseFloat(histogram[Math.min(index+1, size-1)].key);

    return (left + ((right-left)*location)).toFixed(2);
  }

  function getPercentilesWithCI(percentiles, histogram, metric) {

    if ( !percentiles.length ) {
      throw "percentiles must be a non-empty array of integers";
    }

    if ( histogram.values.some(bin => isNaN(bin.key) || isNaN(bin.value) || (bin.key === null) || (bin.value === null)) ) {
      throw "histogram can only contain non-null numeric values";
    }

    var results = [];
    for (const percentile of percentiles) {
      var lower = null;  /* left edge of lower estimate bin */
      var point = null;  /* center of point estimate bin */
      var upper = null;  /* right edge of upper estimate bin */

      if ( histogram.values && histogram && histogramIsNonZero(histogram.values) ) {
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
  SELECT
    'test' AS metric_name,
    STRUCT(
      [
        STRUCT(0.0 AS key, 0.0 AS value),
        STRUCT(1.0 AS key, NULL AS value),
        STRUCT(2.0 AS key, 0.0 AS value)
      ] AS VALUES
    ) AS null_value,
    STRUCT(
      [
        STRUCT(0.0 AS key, 0.0 AS value),
        STRUCT(NULL AS key, 1.0 AS value),
        STRUCT(2.0 AS key, 0.0 AS value)
      ] AS VALUES
    ) AS null_key,
    STRUCT(
      [
        STRUCT(0.0 AS key, 0.0 AS value),
        STRUCT(1.0 AS key, 0.0 AS value),
        STRUCT(2.0 AS key, 0.0 AS value)
      ] AS VALUES
    ) AS all_zero,
    STRUCT(
      [
        STRUCT(0.0 AS key, 0.0 AS value),
        STRUCT(1.0 AS key, 1.0 AS value),
        STRUCT(2.0 AS key, 0.0 AS value)
      ] AS VALUES
    ) AS one_bin,
    STRUCT(
      [
        STRUCT(-4.5 AS key, 0.0 / 100.0 AS value),
        STRUCT(-3.5 AS key, 1.0 / 100.0 AS value),
        STRUCT(-2.5 AS key, 12.0 / 100.0 AS value),
        STRUCT(-1.5 AS key, 22.0 / 100.0 AS value),
        STRUCT(-0.5 AS key, 30.0 / 100.0 AS value),
        STRUCT(0.5 AS key, 22.0 / 100.0 AS value),
        STRUCT(1.5 AS key, 12.0 / 100.0 AS value),
        STRUCT(2.5 AS key, 1.0 / 100.0 AS value),
        STRUCT(3.5 AS key, 0.0 / 100.0 AS value)
      ] AS VALUES
    ) AS seven_bin
),
all_zero_percentiles AS (
  SELECT
    udf_js.bootstrap_percentile_ci([5, 25, 50, 75, 95], all_zero, metric_name) AS output
  FROM
    test_data
),
all_zero_results AS (
  SELECT
    output.*
  FROM
    all_zero_percentiles,
    UNNEST(output) AS output
),
one_bin_percentiles AS (
  SELECT
    udf_js.bootstrap_percentile_ci([5, 25, 50, 75, 95], one_bin, metric_name) AS output
  FROM
    test_data
),
one_bin_results AS (
  SELECT
    output.*
  FROM
    one_bin_percentiles,
    UNNEST(output) AS output
),
seven_bin_percentiles AS (
  SELECT
    udf_js.bootstrap_percentile_ci([5, 25, 50, 75, 95], seven_bin, metric_name) AS output
  FROM
    test_data
),
seven_bin_results AS (
  SELECT
    output.*
  FROM
    seven_bin_percentiles,
    UNNEST(output) AS output
)
-- valid outputs
SELECT
  mozfun.assert.equals('test', (SELECT DISTINCT metric FROM all_zero_results)),
  mozfun.assert.equals('percentile', (SELECT DISTINCT statistic FROM all_zero_results)),
  mozfun.assert.array_equals(
    (SELECT ['5', '25', '50', '75', '95']),
    (SELECT ARRAY(SELECT parameter FROM all_zero_results))
  ),
  mozfun.assert.array_empty((SELECT ARRAY(SELECT lower FROM all_zero_results WHERE lower IS NOT NULL))),
  mozfun.assert.array_empty((SELECT ARRAY(SELECT point FROM all_zero_results WHERE point IS NOT NULL))),
  mozfun.assert.array_empty((SELECT ARRAY(SELECT upper FROM all_zero_results WHERE upper IS NOT NULL))),
  mozfun.assert.equals('test', (SELECT DISTINCT metric FROM one_bin_results)),
  mozfun.assert.equals('percentile', (SELECT DISTINCT statistic FROM one_bin_results)),
  mozfun.assert.array_equals(
    (SELECT ['5', '25', '50', '75', '95']),
    (SELECT ARRAY(SELECT parameter FROM one_bin_results))
  ),
  mozfun.assert.array_equals(
    (SELECT [1.0, 1.0, 1.0, 1.0, 1.0]),
    (SELECT ARRAY(SELECT lower FROM one_bin_results))
  ),
  mozfun.assert.array_equals(
    (SELECT [1.5, 1.5, 1.5, 1.5, 1.5]),
    (SELECT ARRAY(SELECT point FROM one_bin_results))
  ),
  mozfun.assert.array_equals(
    (SELECT [2.0, 2.0, 2.0, 2.0, 2.0]),
    (SELECT ARRAY(SELECT upper FROM one_bin_results))
  ),
  mozfun.assert.equals('test', (SELECT DISTINCT metric FROM seven_bin_results)),
  mozfun.assert.equals('percentile', (SELECT DISTINCT statistic FROM seven_bin_results)),
  mozfun.assert.array_equals(
    (SELECT ['5', '25', '50', '75', '95']),
    (SELECT ARRAY(SELECT parameter FROM seven_bin_results))
  ),
  mozfun.assert.array_equals(
    (SELECT [-2.5, -1.5, -0.5, 0.5, 1.5]),
    (SELECT ARRAY(SELECT lower FROM seven_bin_results))
  ),
  mozfun.assert.array_equals(
    (SELECT [-2.0, -1.0, 0.0, 1.0, 2.0]),
    (SELECT ARRAY(SELECT point FROM seven_bin_results))
  ),
  mozfun.assert.array_equals(
    (SELECT [-1.5, -0.5, 0.5, 1.5, 2.5]),
    (SELECT ARRAY(SELECT upper FROM seven_bin_results))
  );

-- bad percentile arguments
#xfail
SELECT
  udf_js.bootstrap_percentile_ci([], one_bin, metric_name)
FROM
  test_data;

#xfail
SELECT
  udf_js.bootstrap_percentile_ci([-10], one_bin, metric_name)
FROM
  test_data;

#xfail
SELECT
  udf_js.bootstrap_percentile_ci([110], one_bin, metric_name)
FROM
  test_data;

-- malformed histograms
#xfail
SELECT
  udf_js.bootstrap_percentile_ci([50], null_value, metric_name)
FROM
  test_data;

#xfail
SELECT
  udf_js.bootstrap_percentile_ci([50], null_key, metric_name)
FROM
  test_data;
