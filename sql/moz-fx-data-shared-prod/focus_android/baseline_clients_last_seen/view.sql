-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.baseline_clients_last_seen`
AS
SELECT
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus.baseline_clients_last_seen`
UNION ALL
SELECT
  * REPLACE ("beta" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.baseline_clients_last_seen`
UNION ALL
SELECT
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.baseline_clients_last_seen`
