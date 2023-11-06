WITH new_data AS (
  SELECT
    client_id,
    sample_id,
    ad_clicks,
  FROM
    `moz-fx-data-shared-prod`.fenix_derived.attributable_clients_v2
  WHERE
    submission_date = @submission_date
    AND ad_clicks > 0
),
existing_data AS (
  SELECT
    client_id,
    sample_id,
    ad_click_history
  FROM
    `moz-fx-data-shared-prod`.fenix_derived.client_adclicks_history_v1
)
SELECT
  client_id,
  sample_id,
  mozfun.map.set_key(ad_click_history, @submission_date, ad_clicks)
FROM
  existing_data
FULL OUTER JOIN
  new_data
USING
  (sample_id, client_id)
