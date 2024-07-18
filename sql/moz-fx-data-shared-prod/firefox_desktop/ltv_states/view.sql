{% set max_days = 168 %}
{% set death_time = 168 %}
{% set lookback = 28 %}
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.ltv_states`
AS
SELECT
  a.client_id,
  a.sample_id,
  a.submission_date,
  a.first_seen_date,
  a.days_since_first_seen,
  a.days_since_active,
  a.first_reported_country,
  a.attribution,
  a.active,
  a.ad_clicks,
  a.total_historic_ad_clicks BIT_COUNT(
    `mozfun`.bytes.extract_bits(days_seen_bytes, - {{lookback}}, {{lookback}})
  ) AS pattern,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.ltv_states_v1` a
