-- Params Note: Set these same values in fenix.ltv_states
{% set max_weeks = 32 %}
{% set death_time = 168 %}
{% set lookback = 28 %}
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.client_ltv`
AS
WITH extracted_fields AS (
  SELECT
    *,
    BIT_COUNT(
      `mozfun`.bytes.extract_bits(days_seen_bytes, -1 * {{ lookback }}, {{ lookback }})
    ) AS activity_pattern,
    BIT_COUNT(`mozfun`.bytes.extract_bits(days_seen_bytes, -1, 1)) AS active_on_this_date,
  FROM
    `frank-sandbox.fenix_derived.client_ltv_v1`
), with_states AS (
  SELECT
    client_id,
    sample_id,
    as_of_date,
    first_reported_country AS country,
    [
      STRUCT(
        mozfun.ltv.android_states_v1(
          adjust_network,
          days_since_first_seen,
          as_of_date,
          first_seen_date,
          activity_pattern,
          active_on_this_date,
          {{ max_weeks }},
          first_reported_country
        ) AS state,
        'android_states_v1' AS state_function
      ),
      STRUCT(
        mozfun.ltv.android_states_with_paid_v1(
          adjust_network,
          days_since_first_seen,
          as_of_date,
          first_seen_date,
          activity_pattern,
          active_on_this_date,
          {{ max_weeks }},
          first_reported_country
        ) AS state,
        'android_states_with_paid_v1' AS state_function
      ),
      STRUCT(
        mozfun.ltv.android_states_with_paid_v2(
          adjust_network,
          days_since_first_seen,
          days_since_seen,
          {{ death_time }},
          as_of_date,
          first_seen_date,
          activity_pattern,
          active_on_this_date,
          {{ max_weeks }},
          first_reported_country
        ) AS state,
        'android_states_with_paid_v2' AS state_function
      )
    ] AS markov_states,
    * EXCEPT (client_id, sample_id, as_of_date)
  FROM
    extracted_fields
), state_ad_clicks_prediction AS (
  -- When this table is moved to bqetl, these will be enabled as checks
  SELECT
    -- Each country in this table should only have states for one state function
    mozfun.assert.equals(1, COUNT(DISTINCT state_function) OVER (PARTITION BY country)) AS is_valid_countries,
    -- Each country should have each state present only once
    mozfun.assert.equals(1, COUNT(*) OVER (PARTITION BY country, state)) AS is_valid_states,
    *,
  FROM
    mozdata.analysis.android_states_predicted_ad_clicks_v1
)

SELECT
  client_id,
  sample_id,
  country,
  COALESCE(total_historic_ad_clicks, 0) AS total_historic_ad_clicks,
  COALESCE(predicted_ad_clicks, 0) AS total_future_ad_clicks,
  COALESCE(total_historic_ad_clicks, 0) + COALESCE(predicted_ad_clicks, 0) AS total_predicted_ad_clicks,
FROM
  with_states
CROSS JOIN
  UNNEST(markov_states)
JOIN
  state_ad_clicks_prediction
  USING (country, state_function, state)
WHERE
  is_valid_countries AND is_valid_states
