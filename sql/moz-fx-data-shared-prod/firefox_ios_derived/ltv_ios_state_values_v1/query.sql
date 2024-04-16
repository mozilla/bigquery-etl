SELECT
  `state`,
  ltv AS predicted_ad_clicks,
  t AS time_horizon,
  country
FROM
  `mozdata.revenue_cat3_analysis.ios_state_ltvs_v2`
