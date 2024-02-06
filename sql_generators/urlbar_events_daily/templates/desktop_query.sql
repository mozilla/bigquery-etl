WITH
  temp_unnested AS (
  SELECT
    event_id,
    submission_date,
    normalized_channel,
    normalized_country_code,
    res.product_result_type AS type,
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
  )
  WITH temp_session AS (
    SELECT
      event_id,
      submission_date,
      normalized_channel,
      normalized_country_code,
      type,
      COUNTIF(is_clicked) AS is_clicked
      COUNT(*) as n_impressions,
    FROM temp_unnested
    GROUP BY
    1,2
  )
SELECT
  submission_date,
  SUM(n_impressions) AS n_impressions,
  COUNTIF(is_clicked) AS n_clicks,
  normalized_channel,
  normalized_country_code,
  type
FROM
  temp_session
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
  