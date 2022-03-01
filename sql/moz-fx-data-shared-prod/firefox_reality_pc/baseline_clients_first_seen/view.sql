-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality_pc.baseline_clients_first_seen`
AS
SELECT
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefoxreality.baseline_clients_first_seen`
