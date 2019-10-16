CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_clients_daily_v7` AS
SELECT
  *,
  submission_date AS submission_date_s3
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_daily_v7`
