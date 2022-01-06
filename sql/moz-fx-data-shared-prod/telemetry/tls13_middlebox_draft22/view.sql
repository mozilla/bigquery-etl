-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.tls13_middlebox_draft22`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.tls13_middlebox_draft22_v4`
