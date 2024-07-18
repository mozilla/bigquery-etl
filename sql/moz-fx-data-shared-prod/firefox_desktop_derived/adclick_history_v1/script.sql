MERGE INTO
  `moz-fx-data-shared-prod.firefox_desktop_derived.adclick_history_v1` T
  USING (
    SELECT
      s.client_id,
      s.sample_id,
      mozfun.map.set_key(h.ad_click_history, @submission_date, ad_click) AS ad_click_history
    FROM
      `moz-fx-data-shared-prod.search_derived.search_clients_daily_v8` s
    LEFT JOIN
      `moz-fx-data-shared-prod.firefox_desktop_derived.adclick_history_v1` h
      USING (client_id)
    WHERE
      s.submission_date = @submission_date
      AND s.ad_click > 0
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
    SET T.ad_click_history = S.ad_click_history
