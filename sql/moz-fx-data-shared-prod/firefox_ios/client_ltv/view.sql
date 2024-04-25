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
    first_reported_isp
    --add new cols below
    --? AS death_time,
    --? AS pattern,
    -- ? AS active,
    --? AS max_weeks
    --add new cols above
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.client_ltv_v1`
),
states AS (
  SELECT
    e.client_id,
    e.sample_id,
    e.as_of_date,
  --put these into the function
    e.first_seen_date,
    e.days_seen_bytes,
    e.durations,
    e.ad_clicks,
    e.adjust_network,
    e.first_reported_isp,
  --put these into the function
    COALESCE(countries.country, "ROW") AS country,
    mozfun.ltv.get_state_ios_v2(
      e.days_since_first_seen,
      e.days_since_seen,
      e.as_of_date,
      e.death_time,
      e.pattern,
      e.active,
      e.max_weeks
    )
  FROM
    extracted_fields e
  LEFT JOIN
    (SELECT DISTINCT country FROM `moz-fx-data-shared-prod.ltv.ios_state_values`) AS countries
    ON extracted_fields.first_reported_country = countries.country
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
  ?
  USING (country, state_function, state) --join
