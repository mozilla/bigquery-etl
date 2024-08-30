WITH new_data AS (
  SELECT
    client_id,
    SUM(ad_click) AS ad_clicks,
    MAX(profile_group_id) AS profile_group_id
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8`
  WHERE
    submission_date = @submission_date
    AND ad_click > 0
  GROUP BY
    client_id
)
SELECT
  client_id,
  mozfun.map.set_key(ad_click_history, @submission_date, ad_clicks) AS ad_click_history,
  profile_group_id,
FROM
  new_data
FULL OUTER JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.adclick_history_v1`
  USING (client_id)
