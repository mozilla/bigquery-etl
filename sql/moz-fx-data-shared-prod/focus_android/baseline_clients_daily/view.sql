-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.baseline_clients_daily`
AS
SELECT
  "org_mozilla_focus" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus.baseline_clients_daily`
UNION ALL
SELECT
  "org_mozilla_focus_beta" AS normalized_app_id,
  * REPLACE ("beta" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.baseline_clients_daily`
UNION ALL
SELECT
  "org_mozilla_focus_nightly" AS normalized_app_id,
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.baseline_clients_daily`
