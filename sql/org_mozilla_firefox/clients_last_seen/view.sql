CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.clients_last_seen`
AS
SELECT
  * EXCEPT (baseline, metrics),
  baseline.*,
  metrics.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.clients_last_seen_v1`
