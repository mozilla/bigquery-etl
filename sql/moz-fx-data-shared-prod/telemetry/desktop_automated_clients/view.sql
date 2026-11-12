CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_automated_clients`
AS
-- One row per legacy client_id flagged in DENG-11925. Join on client_id for legacy tables, or
-- UNNEST(glean_client_ids) for Glean tables.
WITH flags AS (
  SELECT
    client_id,
    submission_date,
    fleet,
    glean_client_id,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.desktop_automated_clients_v1`
  LEFT JOIN
    UNNEST(glean_client_ids) AS glean_client_id
)
SELECT
  client_id,
  MIN(submission_date) AS first_flagged_date,
  ARRAY_AGG(fleet ORDER BY submission_date LIMIT 1)[SAFE_OFFSET(0)] AS fleet,
  -- A client can be flagged on more than one day with the same Glean client ID. Deduplicate,
  -- so that a join through UNNEST(glean_client_ids) matches each Glean client only once.
  IFNULL(ARRAY_AGG(DISTINCT glean_client_id IGNORE NULLS), []) AS glean_client_ids,
FROM
  flags
GROUP BY
  client_id
