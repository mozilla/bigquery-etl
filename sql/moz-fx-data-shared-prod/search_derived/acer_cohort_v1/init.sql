CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod`.search_derived.acer_cohort_v1
AS
-- Query for search_derived.acer_cohort_v1
-- Pulls all client_ids that were using an Acer distribution in 2017-2022
SELECT DISTINCT
  client_id
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date >= '2017-01-08'
  AND submission_date <= '2023-01-31'
  AND distribution_id IN ('acer', 'acer-001', 'acer-002', 'acer-g-003')
