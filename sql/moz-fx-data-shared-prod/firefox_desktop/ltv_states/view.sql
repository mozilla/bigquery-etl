{% set max_days = 168 %}
{% set death_time = 168 %}
{% set lookback = 28 %}
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.ltv_states`
AS
WITH base AS (
  SELECT
    client_id,
    sample_id,
    submission_date,
    first_seen_date,
    days_since_first_seen,
    days_since_active,
    first_reported_country,
    attribution,
    active,
    ad_clicks,
    total_historic_ad_clicks,
    days_seen_bytes,
    BIT_COUNT(
      `mozfun`.bytes.extract_bits(days_seen_bytes, - {{lookback}}, {{lookback}})
    ) AS pattern,
    {{death_time}} AS death_time,
    {{max_days}} AS max_days,
    {{lookback}} AS lookback,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_derived.ltv_states_v1`
)
SELECT
  client_id,
  sample_id,
  submission_date,
  first_seen_date,
  days_since_first_seen,
  days_since_active,
  first_reported_country,
  attribution,
  active,
  ad_clicks,
  total_historic_ad_clicks,
  days_seen_bytes,
  pattern,
  death_time,
  max_days,
  STRUCT(
    mozfun.ltv.desktop_states_v1(
      days_since_first_seen,
      days_since_active,
      submission_date,
      first_seen_date,
      death_time,
      pattern,
      active,
      max_days,
      lookback
    ) AS desktop_states_v1
  ) AS markov_states
FROM
  base
