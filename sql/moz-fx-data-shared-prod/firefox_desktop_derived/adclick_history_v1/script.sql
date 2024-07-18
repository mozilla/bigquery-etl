MERGE INTO
  `moz-fx-data-shared-prod.firefox_desktop_derived.adclick_history_v1` T
  USING (
    SELECT
      clients_with_clicks_on_submission_date.client_id,
      clients_with_clicks_on_submission_date.sample_id,
      mozfun.map.set_key(
        h.ad_click_history,
        @submission_date,
        clients_with_clicks_on_submission_date.ad_clicks
      ) AS ad_click_history
    FROM
      (
        SELECT
          client_id,
          sample_id,
          ad_click AS ad_clicks
        FROM
          `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8`
        WHERE
          submission_date = @submission_date
          AND ad_click > 0
      ) clients_with_clicks_on_submission_date
    JOIN
      `moz-fx-data-shared-prod.firefox_desktop_derived.adclick_history_v1` h
      USING (sample_id, client_id)
  ) S
  ON T.sample_id = S.sample_id
  AND T.client_id = S.client_id
WHEN NOT MATCHED
THEN
  INSERT
    (client_id, sample_id, ad_click_history)
  VALUES
    (S.client_id, S.sample_id, S.ad_click_history)
  WHEN MATCHED
THEN
  UPDATE
    SET T.ad_click_history = S.ad_click_history,
    T.sample_id = S.sample_id,
    T.client_id = S.client_id
