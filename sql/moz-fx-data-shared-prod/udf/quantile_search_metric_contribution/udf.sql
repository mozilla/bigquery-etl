/*
This function returns how much of one metric is contributed by the quantile of another metric.
Quantile variable should add an offset to get the requried percentile value.

Example: udf.quantile_search_metric_contribution(sap, search_with_ads, sap_percentiles[OFFSET(9)])
It returns search_with_ads if sap value in top 10% volumn else null.
*/
CREATE OR REPLACE FUNCTION udf.quantile_search_metric_contribution(
  metric1 FLOAT64,
  metric2 FLOAT64,
  quantile FLOAT64
) AS (
  IF(metric1 > quantile, metric2, NULL)
);

-- Test
SELECT
  mozfun.assert.equals(1.0, udf.quantile_search_metric_contribution(73, 1, 60)),
  mozfun.assert.null(udf.quantile_search_metric_contribution(29, 9, 60)),
  mozfun.assert.equals(8.0, udf.quantile_search_metric_contribution(118, 8, 21)),
  mozfun.assert.null(udf.quantile_search_metric_contribution(10, 5, 21));
