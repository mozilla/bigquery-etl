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
  sample_id,
  isp,
  days_seen_bytes,
  active,
  {{death_time}} AS death_time,
    BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes , -1 * {{lookback}}, {{lookback}})) AS pattern, 
  {{death_time}} AS max_weeks
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.ltv_states_v1`
