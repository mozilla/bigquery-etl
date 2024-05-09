--Params note: Set these same values in moz-fx-data-shared-prod.ltv.firefox_ios_client_ltv in private BQETL
{% set max_weeks = 32 %}
{% set death_time = 160 %}
{% set lookback = 28 %}
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.ltv_states`
AS
WITH base_layer AS (
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
    ) AS active,
    ad_clicks,
    total_historic_ad_clicks,
    adjust_network,
    first_reported_country,
    first_reported_isp,
    {{ death_time }} AS death_time,
    {{ max_weeks }} AS max_weeks
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.ltv_states_v1`
)
SELECT
  client_id,
  sample_id,
  submission_date,
  first_seen_date,
  days_since_first_seen,
  days_since_seen,
  pattern,
  active,
  ad_clicks,
  total_historic_ad_clicks,
  adjust_network,
  first_reported_country,
  first_reported_isp,
  death_time,
  max_weeks,
  STRUCT(
    mozfun.ltv.get_state_ios_v2(
      days_since_first_seen,
      days_since_seen,
      submission_date,
      death_time,
      pattern,
      active,
      max_weeks
    ) AS state_ios_v2
  ) AS markov_state
FROM
  base_layer
