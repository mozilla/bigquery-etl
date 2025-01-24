WITH temp_unnested AS ( -- dummy change
  SELECT
    submission_date,
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
),
temp_session AS (
  SELECT
    submission_date,
    event_id,
    product_result_type,
    is_terminal,
    ANY_VALUE(normalized_channel) AS normalized_channel,
    ANY_VALUE(normalized_country_code) AS normalized_country_code,
    ANY_VALUE(firefox_suggest_enabled) AS firefox_suggest_enabled,
    ANY_VALUE(sponsored_suggestions_enabled) AS sponsored_suggestions_enabled,
    LOGICAL_OR(is_clicked) AS is_clicked,
    LOGICAL_OR(is_annoyed) AS is_annoyed,
    LOGICAL_OR(is_terminal = TRUE) AS is_impression,
  FROM
    temp_unnested
  GROUP BY
    submission_date,
    event_id,
    product_result_type,
    is_terminal
),
total_urlbar_sessions AS (
  SELECT
    submission_date,
    normalized_country_code,
    normalized_channel,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    COUNT(DISTINCT event_id) AS urlbar_sessions
  FROM
    temp_session
  WHERE
    is_terminal = TRUE
  GROUP BY
    submission_date,
    normalized_country_code,
    normalized_channel,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled
),
daily_counts AS (
  SELECT
    submission_date,
    normalized_country_code,
    normalized_channel,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    product_result_type,
    COUNTIF(is_impression) AS urlbar_impressions,
    COUNTIF(is_clicked) AS urlbar_clicks,
    COUNTIF(is_annoyed) AS urlbar_annoyances,
  FROM
    temp_session
  GROUP BY
    submission_date,
    normalized_country_code,
    normalized_channel,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    product_result_type
)
SELECT
  submission_date,
  normalized_country_code,
  normalized_channel,
  firefox_suggest_enabled,
  sponsored_suggestions_enabled,
  product_result_type,
  urlbar_impressions,
  urlbar_clicks,
  urlbar_annoyances,
  urlbar_sessions
FROM
  daily_counts
LEFT JOIN
  total_urlbar_sessions
  USING (
    submission_date,
    normalized_country_code,
    normalized_channel,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled
  )
