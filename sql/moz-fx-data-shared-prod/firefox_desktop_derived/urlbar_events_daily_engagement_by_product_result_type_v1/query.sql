WITH temp_unnested AS (
  SELECT
    submission_date,
    legacy_telemetry_client_id AS client_id,
    profile_group_id,
    experiments,
    event_id,
    event_action,
    res.product_result_type AS product_result_type,
    normalized_channel,
    normalized_country_code,
    pref_fx_suggestions AS firefox_suggest_enabled,
    pref_sponsored_suggestions AS sponsored_suggestions_enabled,
    is_terminal,
    (
      product_selected_result = res.product_result_type
      AND event_action = 'engaged'
      AND is_terminal
    ) AS is_clicked,
    (product_selected_result = res.product_result_type AND event_action = 'annoyance') AS is_annoyed
  FROM
    `mozdata.firefox_desktop.urlbar_events`
  CROSS JOIN
    UNNEST(results) AS res
  WHERE
    submission_date = @submission_date
)
SELECT
  submission_date,
  client_id,
  profile_group_id,
  product_result_type,
  ANY_VALUE(experiments) AS experiments,
  ANY_VALUE(normalized_channel) AS normalized_channel,
  ANY_VALUE(normalized_country_code) AS normalized_country_code,
  ANY_VALUE(firefox_suggest_enabled) AS firefox_suggest_enabled,
  ANY_VALUE(sponsored_suggestions_enabled) AS sponsored_suggestions_enabled,
  COUNTIF(is_clicked) AS urlbar_clicks,
  COUNTIF(is_annoyed) AS urlbar_annoyances,
  COUNTIF(is_terminal = TRUE) AS urlbar_impressions,
FROM
  temp_unnested
GROUP BY
  submission_date,
  client_id,
  profile_group_id,
  product_result_type
