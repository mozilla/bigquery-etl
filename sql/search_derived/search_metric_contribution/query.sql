-- temp function returns how much of one metric is contributed by the quantile of another metric
CREATE TEMP FUNCTION get_quantile(metric1 FLOAT64, metric2 FLOAT64, quantile FLOAT64) AS (
  IF(metric1 > quantile, metric2, NULL)
);

WITH monthly_client_aggregates AS (
  SELECT
    client_id,
    SUM(sap) AS sap,
    SUM(search_with_ads) AS search_with_ads,
    SUM(active_hours_sum) AS active_hours_sum,
    SUM(ad_click) AS ad_click,
  FROM
    `moz-fx-data-shared-prod.search.search_clients_daily`
  WHERE
    submission_date >= DATE_SUB(@submission_date, INTERVAL 27 DAY)
  GROUP BY
    1
),
monthly_aggregates AS (
  SELECT
    SUM(sap) AS sap,
    SUM(search_with_ads) AS search_with_ads,
    SUM(active_hours_sum) AS active_hours_sum,
    SUM(ad_click) AS ad_click
  FROM
    monthly_client_aggregates
),
metric_percentile_cut AS (
  SELECT
    APPROX_QUANTILES(ad_click, 10) AS ad_click_percentiles,
    APPROX_QUANTILES(search_with_ads, 10) AS search_with_ads_percentiles,
    APPROX_QUANTILES(active_hours_sum, 10) AS active_hours_percentiles,
    APPROX_QUANTILES(sap, 10) AS sap_percentiles
  FROM
    monthly_client_aggregates
),
-- metric aggregates from metric
metric_aggregates_from_metric AS (
  SELECT
    SUM(
      get_quantile(sap, search_with_ads, sap_percentiles[OFFSET(9)])
    ) AS search_with_ads_from_sap_top_10,
    SUM(
      get_quantile(sap, search_with_ads, sap_percentiles[OFFSET(8)])
    ) AS search_with_ads_from_sap_top_20,
    SUM(
      get_quantile(sap, search_with_ads, sap_percentiles[OFFSET(7)])
    ) AS search_with_ads_from_sap_top_30,
    SUM(
      get_quantile(active_hours_sum, search_with_ads, active_hours_percentiles[OFFSET(9)])
    ) AS search_with_ads_from_active_hours_top_10,
    SUM(
      get_quantile(active_hours_sum, search_with_ads, active_hours_percentiles[OFFSET(8)])
    ) AS search_with_ads_from_active_hours_top_20,
    SUM(
      get_quantile(active_hours_sum, search_with_ads, active_hours_percentiles[OFFSET(7)])
    ) AS search_with_ads_from_active_hours_top_30,
    SUM(
      get_quantile(ad_click, search_with_ads, ad_click_percentiles[OFFSET(9)])
    ) AS search_with_ads_from_ad_click_top_10,
    SUM(
      get_quantile(ad_click, search_with_ads, ad_click_percentiles[OFFSET(8)])
    ) AS search_with_ads_from_ad_click_top_20,
    SUM(
      get_quantile(ad_click, search_with_ads, ad_click_percentiles[OFFSET(7)])
    ) AS search_with_ads_from_ad_click_top_30,
    SUM(get_quantile(sap, ad_click, sap_percentiles[OFFSET(9)])) AS ad_click_from_sap_top_10,
    SUM(get_quantile(sap, ad_click, sap_percentiles[OFFSET(8)])) AS ad_click_from_sap_top_20,
    SUM(get_quantile(sap, ad_click, sap_percentiles[OFFSET(7)])) AS ad_click_from_sap_top_30,
    SUM(
      get_quantile(search_with_ads, ad_click, search_with_ads_percentiles[OFFSET(9)])
    ) AS ad_click_from_search_with_ads_top_10,
    SUM(
      get_quantile(search_with_ads, ad_click, search_with_ads_percentiles[OFFSET(8)])
    ) AS ad_click_from_search_with_ads_top_20,
    SUM(
      get_quantile(search_with_ads, ad_click, search_with_ads_percentiles[OFFSET(7)])
    ) AS ad_click_from_search_with_ads_top_30,
    SUM(
      get_quantile(active_hours_sum, ad_click, active_hours_percentiles[OFFSET(9)])
    ) AS ad_click_from_active_hours_top_10,
    SUM(
      get_quantile(active_hours_sum, ad_click, active_hours_percentiles[OFFSET(8)])
    ) AS ad_click_from_active_hours_top_20,
    SUM(
      get_quantile(active_hours_sum, ad_click, active_hours_percentiles[OFFSET(7)])
    ) AS ad_click_from_active_hours_top_30,
    SUM(
      get_quantile(sap, active_hours_sum, sap_percentiles[OFFSET(9)])
    ) AS active_hours_from_sap_top_10,
    SUM(
      get_quantile(sap, active_hours_sum, sap_percentiles[OFFSET(8)])
    ) AS active_hours_from_sap_top_20,
    SUM(
      get_quantile(sap, active_hours_sum, sap_percentiles[OFFSET(7)])
    ) AS active_hours_from_sap_top_30,
    SUM(
      get_quantile(ad_click, active_hours_sum, ad_click_percentiles[OFFSET(9)])
    ) AS active_hours_from_ad_click_top_10,
    SUM(
      get_quantile(ad_click, active_hours_sum, ad_click_percentiles[OFFSET(8)])
    ) AS active_hours_from_ad_click_top_20,
    SUM(
      get_quantile(ad_click, active_hours_sum, ad_click_percentiles[OFFSET(7)])
    ) AS active_hours_from_ad_click_top_30,
    SUM(
      get_quantile(search_with_ads, active_hours_sum, search_with_ads_percentiles[OFFSET(9)])
    ) AS active_hours_from_search_with_ads_top_10,
    SUM(
      get_quantile(search_with_ads, active_hours_sum, search_with_ads_percentiles[OFFSET(8)])
    ) AS active_hours_from_search_with_ads_top_20,
    SUM(
      get_quantile(search_with_ads, active_hours_sum, search_with_ads_percentiles[OFFSET(7)])
    ) AS active_hours_from_search_with_ads_top_30,
    SUM(
      get_quantile(search_with_ads, sap, search_with_ads_percentiles[OFFSET(9)])
    ) AS sap_from_search_with_ads_top_10,
    SUM(
      get_quantile(search_with_ads, sap, search_with_ads_percentiles[OFFSET(8)])
    ) AS sap_from_search_with_ads_top_20,
    SUM(
      get_quantile(search_with_ads, sap, search_with_ads_percentiles[OFFSET(7)])
    ) AS sap_from_search_with_ads_top_30,
    SUM(get_quantile(ad_click, sap, ad_click_percentiles[OFFSET(9)])) AS sap_from_ad_click_top_10,
    SUM(get_quantile(ad_click, sap, ad_click_percentiles[OFFSET(8)])) AS sap_from_ad_click_top_20,
    SUM(get_quantile(ad_click, sap, ad_click_percentiles[OFFSET(7)])) AS sap_from_ad_click_top_30,
    SUM(
      get_quantile(active_hours_sum, sap, active_hours_percentiles[OFFSET(9)])
    ) AS sap_from_active_hours_top_10,
    SUM(
      get_quantile(active_hours_sum, sap, active_hours_percentiles[OFFSET(8)])
    ) AS sap_from_active_hours_top_20,
    SUM(
      get_quantile(active_hours_sum, sap, active_hours_percentiles[OFFSET(7)])
    ) AS sap_from_active_hours_top_30
  FROM
    monthly_client_aggregates,
    metric_percentile_cut
),
-- metrics percentage calculation
metric_from_metric_pct AS (
  SELECT
    @submission_date AS submission_date,
    STRUCT(
      search_with_ads_from_sap_top_10 / search_with_ads AS top_10,
      search_with_ads_from_sap_top_20 / search_with_ads AS top_20,
      search_with_ads_from_sap_top_30 / search_with_ads AS top_30
    ) AS search_with_ads_from_sap,
    STRUCT(
      search_with_ads_from_active_hours_top_10 / search_with_ads AS top_10,
      search_with_ads_from_active_hours_top_20 / search_with_ads AS top_20,
      search_with_ads_from_active_hours_top_30 / search_with_ads AS top_30
    ) AS search_with_ads_from_active_hours,
    STRUCT(
      search_with_ads_from_ad_click_top_10 / search_with_ads AS top_10,
      search_with_ads_from_ad_click_top_20 / search_with_ads AS top_20,
      search_with_ads_from_ad_click_top_30 / search_with_ads AS top_30
    ) AS search_with_ads_from_ad_click,
    STRUCT(
      ad_click_from_sap_top_10 / ad_click AS top_10,
      ad_click_from_sap_top_20 / ad_click AS top_20,
      ad_click_from_sap_top_30 / ad_click AS top_30
    ) AS ad_click_from_sap,
    STRUCT(
      ad_click_from_active_hours_top_10 / ad_click AS top_10,
      ad_click_from_active_hours_top_20 / ad_click AS top_20,
      ad_click_from_active_hours_top_30 / ad_click AS top_30
    ) AS ad_click_from_active_hours,
    STRUCT(
      ad_click_from_search_with_ads_top_10 / ad_click AS top_10,
      ad_click_from_search_with_ads_top_20 / ad_click AS top_20,
      ad_click_from_search_with_ads_top_30 / ad_click AS top_30
    ) AS ad_click_from_search_with_ads,
    STRUCT(
      active_hours_from_sap_top_10 / active_hours_sum AS top_10,
      active_hours_from_sap_top_20 / active_hours_sum AS top_20,
      active_hours_from_sap_top_30 / active_hours_sum AS top_30
    ) AS active_hours_from_sap,
    STRUCT(
      active_hours_from_ad_click_top_10 / active_hours_sum AS top_10,
      active_hours_from_ad_click_top_20 / active_hours_sum AS top_20,
      active_hours_from_ad_click_top_30 / active_hours_sum AS top_30
    ) AS active_hours_from_ad_click,
    STRUCT(
      active_hours_from_search_with_ads_top_10 / active_hours_sum AS top_10,
      active_hours_from_search_with_ads_top_20 / active_hours_sum AS top_20,
      active_hours_from_search_with_ads_top_30 / active_hours_sum AS top_30
    ) AS active_hours_from_search_with_ads,
    STRUCT(
      sap_from_search_with_ads_top_10 / sap AS top_10,
      sap_from_search_with_ads_top_20 / sap AS top_20,
      sap_from_search_with_ads_top_30 / sap AS top_30
    ) AS sap_from_search_with_ads,
    STRUCT(
      sap_from_ad_click_top_10 / sap AS top_10,
      sap_from_ad_click_top_20 / sap AS top_20,
      sap_from_ad_click_top_30 / sap AS top_30
    ) AS sap_from_ad_click,
    STRUCT(
      sap_from_active_hours_top_10 / sap AS top_10,
      sap_from_active_hours_top_20 / sap AS top_20,
      sap_from_active_hours_top_30 / sap AS top_30
    ) AS sap_from_active_hours
  FROM
    monthly_aggregates,
    metric_aggregates_from_metric
)
-- final query
SELECT
  *
FROM
  metric_from_metric_pct
