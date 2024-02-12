WITH
  temp_unnested AS (
  SELECT
    submission_date,
    event_id,
    res.product_result_type AS product_result_type,
    normalized_channel,
    normalized_country_code,
    pref_fx_suggestions,
    pref_sponsored_suggestions,
    CASE
      WHEN product_selected_result = res.product_result_type THEN 1
    ELSE
    0
  END
    AS is_clicked
  FROM
    `mozdata.firefox_desktop.urlbar_events`
  CROSS JOIN
    UNNEST (results) AS res
  WHERE
    submission_date = @submission_date
    AND is_terminal = TRUE
  ),
  temp_session AS (
    SELECT
      submission_date,
      event_id,
      product_result_type,
      mozdata.udf.mode_last(ARRAY_AGG(normalized_channel)) AS normalized_channel,
      mozdata.udf.mode_last(ARRAY_AGG(normalized_country_code)) AS normalized_country_code,
      mozdata.udf.mode_last(ARRAY_AGG(pref_fx_suggestions)) AS pref_fx_suggestions,
      mozdata.udf.mode_last(ARRAY_AGG(pref_sponsored_suggestions)) AS pref_sponsored_suggestions,
      LOGICAL_OR(event_action = 'engaged' AND is_clicked > 0) AS is_clicked,
      1 as n_impressions,
    FROM temp_unnested
    GROUP BY
    submission_date,
    event_id,
    product_result_type
  )
SELECT
  submission_date,
  normalized_country_code,
  normalized_channel,
  pref_fx_suggestions as firefox_suggest_enabled,
  pref_sponsored_suggestions as sponsored_suggestions_enabled,
  product_result_type,
  SUM(n_impressions) AS urlbar_impressions,
  COUNTIF(is_clicked > 0) AS urlbar_clicks
FROM
  temp_session
GROUP BY
  submission_date,
  normalized_country_code,
  firefox_suggest_enabled,
  sponsored_suggestions_enabled,
  product_result_type,
  normalized_channel
ORDER BY
  submission_date DESC,
  normalized_country_code DESC,
  firefox_suggest_enabled DESC,
  sponsored_suggestions_enabled DESC,
  normalized_channel DESC,
  n_impressions DESC
