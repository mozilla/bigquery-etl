WITH
  temp_unnested AS (
  SELECT
    submission_date,
    event_id,
    event_action,
    res.product_result_type AS product_result_type,
    normalized_channel,
    normalized_country_code,
    pref_fx_suggestions,
    pref_sponsored_suggestions,
    is_terminal,
    CASE
      WHEN (product_selected_result = res.product_result_type AND event_action = 'engaged' AND is_terminal = TRUE) THEN 1
    ELSE
    0
  END
    AS is_clicked,
    CASE
      WHEN (product_selected_result = res.product_result_type AND event_action = 'annoyance') THEN 1
    ELSE
    0
  END
    AS is_annoyed,
  FROM
    `mozdata.firefox_desktop.urlbar_events`
  CROSS JOIN
    UNNEST (results) AS res
  WHERE
    submission_date = @submission_date ),
  temp_session AS (
  SELECT
    submission_date,
    event_id,
    product_result_type,
    is_terminal,
    mozdata.udf.mode_last(ARRAY_AGG(normalized_channel)) AS normalized_channel,
    mozdata.udf.mode_last(ARRAY_AGG(normalized_country_code)) AS normalized_country_code,
    mozdata.udf.mode_last(ARRAY_AGG(pref_fx_suggestions)) AS pref_fx_suggestions,
    mozdata.udf.mode_last(ARRAY_AGG(pref_sponsored_suggestions)) AS pref_sponsored_suggestions,
    LOGICAL_OR(is_clicked > 0) AS is_clicked,
    LOGICAL_OR(is_annoyed > 0) AS is_annoyed,
    LOGICAL_OR(is_terminal = TRUE) AS n_impressions,
  FROM
    temp_unnested
  GROUP BY
    submission_date,
    event_id,
    product_result_type,
    is_terminal ),
  total_urlbar_sessions AS (
  SELECT
    submission_date,
    normalized_country_code,
    normalized_channel,
    COUNT(DISTINCT event_id) AS urlbar_sessions
  FROM
    temp_session
  WHERE
    is_terminal = TRUE
  GROUP BY
    submission_date,
    normalized_country_code,
    normalized_channel ),
  daily_counts AS (
  SELECT
    submission_date,
    normalized_country_code,
    normalized_channel,
    pref_fx_suggestions AS firefox_suggest_enabled,
    pref_sponsored_suggestions AS sponsored_suggestions_enabled,
    product_result_type,
    COUNTIF(n_impressions) AS urlbar_impressions,
    COUNTIF(is_clicked) AS urlbar_clicks,
    COUNTIF(is_annoyed) AS urlbar_annoyances,
  FROM
    temp_session
  GROUP BY
    submission_date,
    normalized_country_code,
    firefox_suggest_enabled,
    sponsored_suggestions_enabled,
    product_result_type,
    normalized_channel)
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
USING
  ( submission_date,
    normalized_country_code,
    normalized_channel)
ORDER BY
  submission_date DESC,
  normalized_country_code DESC,
  normalized_channel DESC,
  firefox_suggest_enabled DESC,
  sponsored_suggestions_enabled DESC,
  urlbar_impressions DESC
