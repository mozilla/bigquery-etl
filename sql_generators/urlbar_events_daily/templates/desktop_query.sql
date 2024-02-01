WITH
  temp_unnested AS (
  SELECT
    submission_date,
    normalized_channel,
    normalized_country_code,
    mozfun.norm.result_type_to_product_name(res.result_type) AS type,
    is_terminal,
    CASE
      WHEN selected_result = res.result_type THEN 1
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
  )
SELECT
  submission_date,
  COUNT(*) AS n_impressions,
  SUM(is_clicked) AS n_clicks,
  -- SAFE_DIVIDE(SUM(is_clicked), COUNT(*)) AS CTR,
  normalized_channel,
  normalized_country_code,
  type,
  CASE
  -- is 'xchannels_add_on' considered addon or separate?
    WHEN type IN ('history', 'bookmark', 'open_tab', 'admarketplace_sponsored', 'navigational', 'suggest_add_on', 'wikipedia_enhanced', 'wikipedia_dynamic', 'weather', 'quick_action', 'pocket_collection') THEN TRUE
    WHEN type IN ('default_partner_search_suggestion',
    'search_engine',
    'trending_suggestion',
    'tab_to_search') THEN FALSE
  ELSE
  NULL
END
  AS is_fx_suggest
FROM
  temp_unnested
GROUP BY
  submission_date,
  normalized_country_code,
  type,
  normalized_channel
ORDER BY
  submission_date DESC,
  normalized_country_code DESC,
  n_impressions DESC,
  type ASC,
  normalized_channel