WITH
  temp_unnested AS (
  SELECT
    submission_date,
    event_id,
    normalized_channel,
    normalized_country_code,
    res.product_result_type AS product_result_type,
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
    AND event_action = 'engaged'
    AND is_terminal = TRUE
  ),
  temp_session AS (
    SELECT
      submission_date,
      event_id,
      product_result_type,
      mozdata.udf.mode_last(ARRAY_AGG(normalized_channel)) AS normalized_channel,
      mozdata.udf.mode_last(ARRAY_AGG(normalized_country_code)) AS normalized_country_code,
      COUNTIF(is_clicked > 0) AS is_clicked,
      COUNT(*) as n_impressions,
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
  product_result_type,
  SUM(n_impressions) AS n_impressions,
  COUNTIF(is_clicked > 0) AS n_clicks
FROM
  temp_session
GROUP BY
  submission_date,
  normalized_country_code,
  product_result_type,
  normalized_channel
ORDER BY
  submission_date DESC,
  normalized_country_code DESC,
  n_impressions DESC
