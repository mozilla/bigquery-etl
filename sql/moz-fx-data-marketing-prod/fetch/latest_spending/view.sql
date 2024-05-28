CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.fetch.latest_spending`
AS
SELECT
  *,
FROM
  `moz-fx-data-marketing-prod.fetch.latest_trafficking`
LEFT JOIN
  `moz-fx-data-marketing-prod.fetch.latest_metric`
  USING (fetch_id, AdName)
