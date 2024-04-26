--Params Note: Set these same values in firefox_ios.ltv_states
{% set max_weeks = 32 %}
{% set death_time = 160 %}
{% set lookback = 28 %}
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.client_ltv`
AS
WITH extracted_fields AS (
  SELECT
    client_id,
    sample_id,
    as_of_date,
    first_seen_date,
    days_since_first_seen,
    days_since_seen,
    days_seen_bytes,
    durations,
    ad_clicks,
    adjust_network,
    first_reported_country,
    first_reported_isp,
    {{ death_time }} AS death_time,
    {{ max_weeks }} AS max_weeks,
    BIT_COUNT(
      `mozfun`.bytes.extract_bits(days_seen_bytes, - {{lookback}}, {{lookback}})
    ) AS pattern,
    IF(
      (durations > 0)
      AND (BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes, -1, 1)) = 1),
      1,
      0
    ) AS active
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.client_ltv_v1`
),
states AS (
  SELECT
    e.client_id,
    e.sample_id,
    e.as_of_date,
    e.first_seen_date,
    COALESCE(countries.country, "ROW") AS country,
    [
      STRUCT(
        mozfun.ltv.get_state_ios_v2(
          e.days_since_first_seen,
          e.days_since_seen,
          e.as_of_date,
          e.death_time,
          e.pattern,
          e.active,
          e.max_weeks
        ) AS `state`,
        'get_state_ios_v2' AS state_function
      )
    ] AS markov_states
  FROM
    extracted_fields e
  LEFT JOIN
    (SELECT DISTINCT country FROM `moz-fx-data-shared-prod.ltv.ios_state_values`) AS countries
    ON COALESCE(e.first_reported_country, 'ROW') = countries.country
)
SELECT
  client_id,
  sample_id,
  country,
  COALESCE(total_historic_ad_clicks, 0) AS total_historic_ad_clicks,
  COALESCE(predicted_ad_clicks, 0) AS total_future_ad_clicks,
  COALESCE(total_historic_ad_clicks, 0) + COALESCE(
    predicted_ad_clicks,
    0
  ) AS total_predicted_ad_clicks,
FROM
  states
CROSS JOIN
  UNNEST(markov_states)
JOIN
  `moz-fx-data-shared-prod.ltv.ios_state_values`
  USING (country, state_function, state)
