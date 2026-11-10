CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_automated_clients`
AS
-- One row per legacy client_id flagged in DENG-11925. Join on client_id for legacy tables, or
-- UNNEST(glean_client_ids) for Glean tables.
SELECT
  client_id,
  MIN(submission_date) AS first_flagged_date,
  ARRAY_AGG(fleet ORDER BY submission_date LIMIT 1)[SAFE_OFFSET(0)] AS fleet,
  ARRAY_CONCAT_AGG(glean_client_ids) AS glean_client_ids,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_automated_clients_v1`
GROUP BY
  client_id
