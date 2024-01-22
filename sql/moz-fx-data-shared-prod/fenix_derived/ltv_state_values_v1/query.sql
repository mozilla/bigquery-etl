SELECT
  state,
  ltv AS predicted_ad_clicks,
  t AS time_horizon,
  country,
  CASE
    WHEN country IN ('IN', 'US', 'CA', 'DE', 'BE', 'FR', 'GB', 'CH',  'NL', 'ES', 'AT', 'MX', 'PL', 'IT') THEN 'android_states_with_paid_v1'
    WHEN country IN ('BR', 'KE', 'AU', 'JP') THEN 'android_states_v1'
    ELSE NULL
  END AS state_function
FROM
  mozdata.analysis.android_state_ltvs_v1
