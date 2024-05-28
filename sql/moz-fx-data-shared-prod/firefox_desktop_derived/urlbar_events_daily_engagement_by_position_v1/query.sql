SELECT
  submission_date,
  normalized_channel,
  normalized_country_code,
  pref_fx_suggestions AS firefox_suggest_enabled,
  pref_sponsored_suggestions AS sponsored_suggestions_enabled,
  selected_position,
  COUNT(DISTINCT event_id) AS num_clicks
FROM
  `mozdata.firefox_desktop.urlbar_events`
WHERE
  submission_date = @submission_date
  AND event_action = 'engaged'
  AND is_terminal
GROUP BY
  submission_date,
  normalized_channel,
  normalized_country_code,
  firefox_suggest_enabled,
  sponsored_suggestions_enabled,
  selected_position
