-- Query for search_derived.acer_cohort_v1
-- Pulls all client_ids that were using an Acer distribution in late 2022
SELECT DISTINCT
  client_id
FROM
  `moz-fx-data-shared-prod.telemetry.clients_daily`
WHERE
  submission_date >= '2022-12-15'
  AND distribution_id IN ('acer', 'acer-001', 'acer-002', 'acer-g-003')
