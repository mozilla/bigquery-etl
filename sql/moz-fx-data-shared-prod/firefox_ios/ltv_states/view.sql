{% set max_weeks = 32 %}
{% set death_time = 160 %}
{% set lookback = 28 %}

CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.ltv_states`
AS
SELECT
client_id,
sample_id,
submission_date,
first_seen_date,
days_since_first_seen,
days_since_seen,
BIT_COUNT(
  `mozfun`.bytes.extract_bits(days_seen_bytes, - {{lookback}}, {{lookback}})
) AS pattern,
IF(
  (durations > 0)
  AND (BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes, -1, 1)) = 1),
  1,
  0
)  AS active,
ad_clicks,
adjust_network,
first_reported_country,
first_reported_isp 
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.ltv_states_v1`