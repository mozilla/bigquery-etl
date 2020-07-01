WITH with_exploded_history AS (
  SELECT
    history,
    *
  FROM
    `moz-fx-data-shared-prod.analysis.ltv_daily`,
    UNNEST(engine_searches.list) AS history
  WHERE
    submission_date = @submission_date
    AND channel != 'esr'
    AND history.element.key IS NOT NULL
),
with_exploded_months AS (
  /*
  Each client has activity across the past 12 months for each of the major
  search engines. We first unnest the engines to get the engine and its
  associated activity; we then unnest that activity to get individual
  counts per-month.

  The cruft of element/list is because of the Spark connector
  creating these tables.

  This will have 1-row per-client-engine-month.
  */
  SELECT
    client_id,
    history.element.key AS engine,
    DATE_SUB(DATE_TRUNC(submission_date, MONTH), INTERVAL(11 - off) MONTH) AS month,
    country,
    history.element.value.ad_click.list[OFFSET(off)].element AS ad_clicks,
    history.element.value.search_with_ads.list[OFFSET(off)].element AS search_with_ads,
    history.element.value.total_searches.list[OFFSET(off)].element AS sap,
    history.element.value.tagged_searches.list[OFFSET(off)].element AS tagged_sap,
  FROM
    with_exploded_history,
    UNNEST(GENERATE_ARRAY(0, 11)) AS off
),
monthly_activity AS (
  -- Sum the activity across engine, month, and country
  SELECT
    engine,
    month,
    `moz-fx-data-shared-prod`.udf.map_revenue_country(engine, country) AS country,
    SUM(ad_clicks) AS ad_clicks,
    SUM(search_with_ads) AS search_with_ads,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
  FROM
    with_exploded_months
  GROUP BY
    engine,
    month,
    country
),
past_year_revenue AS (
  -- Get the revenue per engine-month-country
  SELECT
    partner_name AS engine,
    DATE(month_year) AS month,
    IF(
      partner_name = 'Bing',
      `moz-fx-data-shared-prod`.udf.map_bing_revenue_country_to_country_code(country),
      country
    ) AS country,
    SUM(revenue_paid_to_mozilla) AS revenue
  FROM
    `dp2-prod`.revenue.revenue_data
  WHERE
   -- Filter to data before the current month, and 12 months ago from the 1st of the current month
    DATE(month_year)
    BETWEEN DATE_SUB(DATE_TRUNC(@submission_date, month), INTERVAL 12 MONTH)
    AND DATE_TRUNC(@submission_date, month)
    AND (
      (partner_name = 'Google' AND device = 'desktop' AND channel = 'personal')
      OR (
        partner_name = 'Bing'
        AND partner_code IN ('Bing_MOZ4', 'Bing_MOZA', 'Bing_MOZB', 'Bing_MOZT', 'Bing_MOZW')
      )
    )
  GROUP BY
    engine,
    month,
    country
),
engine_action_values AS (
  SELECT
    *,
    SAFE_DIVIDE(revenue, ad_clicks) AS ad_click_value,
    SAFE_DIVIDE(revenue, search_with_ads) AS search_with_ads_value,
    SAFE_DIVIDE(revenue, sap) AS sap_value,
    SAFE_DIVIDE(revenue, tagged_sap) AS tagged_sap_value,
  FROM
    monthly_activity
  INNER JOIN
    past_year_revenue
  USING
    (engine, month, country)
),
history_join AS (
  -- Get the average value of every action for that client
  -- Based on their historical values
  SELECT
    client_id,
    country,
    engine,
    AVG(h.ad_clicks * ad_click_value) AS avg_client_ad_click_value,
    AVG(h.search_with_ads * search_with_ads_value) AS avg_client_search_with_ads_value,
    AVG(h.sap * sap_value) AS avg_client_search_value,
    AVG(h.tagged_sap * tagged_sap_value) AS avg_client_tagged_search_value
  FROM
    with_exploded_months h
  INNER JOIN
    engine_action_values
  USING
    (engine, month, country)
  GROUP BY
    client_id,
    country,
    engine
),
past_year_metric_sums AS (
  SELECT
    client_id,
  -- One entry per-engine
    history.element.key AS engine,
  --Sum historical metrics
    `moz-fx-data-shared-prod`.udf.parquet_array_sum(
      history.element.value.total_searches.list
    ) AS total_client_searches_past_year,
    `moz-fx-data-shared-prod`.udf.parquet_array_sum(
      history.element.value.tagged_searches.list
    ) AS total_client_tagged_searches_past_year,
    `moz-fx-data-shared-prod`.udf.parquet_array_sum(
      history.element.value.ad_click.list
    ) AS total_client_ad_clicks_past_year,
    `moz-fx-data-shared-prod`.udf.parquet_array_sum(
      history.element.value.search_with_ads.list
    ) AS total_client_searches_with_ads_past_year,
  -- Frequency for every metric
    COALESCE(days_clicked_ads.frequency, 0) AS ad_click_days,
    COALESCE(days_searched.frequency, 0) AS search_days,
    COALESCE(days_searched_with_ads.frequency, 0) AS search_with_ads_days,
    COALESCE(days_tagged_searched.frequency, 0) AS tagged_search_days,
    COALESCE(days_seen.frequency, 0) AS active_days,
  -- Predictions for each metric
    COALESCE(
      `moz-fx-data-shared-prod`.udf.get_key(predictions.key_value, 'days_clicked_ads'),
      0
    ) AS pred_num_days_clicking_ads,
    COALESCE(
      `moz-fx-data-shared-prod`.udf.get_key(predictions.key_value, 'days_searched_with_ads'),
      0
    ) AS pred_num_days_seeing_ads,
    COALESCE(
      `moz-fx-data-shared-prod`.udf.get_key(predictions.key_value, 'days_searched'),
      0
    ) AS pred_num_days_searching,
    COALESCE(
      `moz-fx-data-shared-prod`.udf.get_key(predictions.key_value, 'days_tagged_searched'),
      0
    ) AS pred_num_days_tagged_searching,
  FROM
    with_exploded_history
),
with_actions_per_day AS (
  -- Get the average number of actions per-day for that client on that engine
  SELECT
    *,
    SAFE_DIVIDE(total_client_ad_clicks_past_year, ad_click_days) AS ad_clicks_per_day,
    SAFE_DIVIDE(
      total_client_searches_with_ads_past_year,
      search_with_ads_days
    ) AS searches_with_ads_per_day,
    SAFE_DIVIDE(total_client_searches_past_year, search_days) AS searches_per_day,
    SAFE_DIVIDE(
      total_client_tagged_searches_past_year,
      tagged_search_days
    ) AS tagged_searches_per_day,
  FROM
    past_year_metric_sums
  INNER JOIN
    history_join
  USING
    (client_id, engine)
),
cutoffs AS (
  -- 99.5% cutoffs
  SELECT
    engine,
    country,
    APPROX_QUANTILES(ad_clicks_per_day, 1000)[OFFSET(995)] AS ad_clicks_cutoff,
    APPROX_QUANTILES(searches_with_ads_per_day, 1000)[OFFSET(995)] AS searches_with_ads_cutoff,
    APPROX_QUANTILES(searches_per_day, 1000)[OFFSET(995)] AS searches_cutoff,
    APPROX_QUANTILES(tagged_searches_per_day, 1000)[OFFSET(995)] AS tagged_searches_cutoff
  FROM
    with_actions_per_day
  GROUP BY
    engine,
    country
),
with_caps AS (
  SELECT
    *,
    LEAST(ad_clicks_per_day, ad_clicks_cutoff) AS ad_clicks_per_day_capped,
    LEAST(searches_with_ads_per_day, searches_with_ads_cutoff) AS searches_with_ads_per_day_capped,
    LEAST(searches_per_day, searches_cutoff) AS searches_per_day_capped,
    LEAST(tagged_searches_per_day, tagged_searches_cutoff) AS tagged_searches_per_day_capped,
  FROM
    with_actions_per_day
  INNER JOIN
    cutoffs
  USING
    (engine, country)
),
with_ltv AS (
  SELECT
    *,
    ad_click_days * ad_clicks_per_day_capped * avg_client_ad_click_value AS ltv_ad_clicks_current,
    search_with_ads_days * searches_with_ads_per_day_capped * avg_client_search_with_ads_value AS ltv_search_with_ads_current,
    search_days * searches_per_day_capped * avg_client_search_value AS ltv_search_current,
    tagged_search_days * tagged_searches_per_day_capped * avg_client_tagged_search_value AS ltv_tagged_search_current,
    pred_num_days_clicking_ads * ad_clicks_per_day * avg_client_ad_click_value AS ltv_ad_clicks_future,
    pred_num_days_seeing_ads * searches_with_ads_per_day_capped * avg_client_search_with_ads_value AS ltv_search_with_ads_future,
    pred_num_days_searching * searches_per_day_capped * avg_client_search_value AS ltv_search_future,
    pred_num_days_tagged_searching * tagged_searches_per_day_capped * avg_client_tagged_search_value AS ltv_tagged_search_future,
  FROM
    with_caps
)
SELECT
  @submission_date AS submission_date,
  *,
  SAFE_DIVIDE(
    ltv_ad_clicks_current,
    SUM(ltv_ad_clicks_current) OVER ()
  ) AS normalized_ltv_ad_clicks_current,
  SAFE_DIVIDE(
    ltv_search_with_ads_current,
    SUM(ltv_search_with_ads_current) OVER ()
  ) AS normalized_ltv_search_with_ads_current,
  SAFE_DIVIDE(ltv_search_current, SUM(ltv_search_current) OVER ()) AS normalized_ltv_search_current,
  SAFE_DIVIDE(
    ltv_ad_clicks_future,
    SUM(ltv_ad_clicks_future) OVER ()
  ) AS normalized_ltv_ad_clicks_future,
  SAFE_DIVIDE(
    ltv_search_with_ads_future,
    SUM(ltv_search_with_ads_future) OVER ()
  ) AS normalized_ltv_search_with_ads_future,
  SAFE_DIVIDE(ltv_search_future, SUM(ltv_search_future) OVER ()) AS normalized_ltv_search_future,
  SAFE_DIVIDE(
    ltv_tagged_search_future,
    SUM(ltv_tagged_search_future) OVER ()
  ) AS normalized_ltv_tagged_search_future
FROM
  with_ltv
