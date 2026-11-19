CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.ga_clients`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_clients_v1`
WHERE
  -- Excluding events from the testing phase.
  first_seen_date >= "2025-07-16"
