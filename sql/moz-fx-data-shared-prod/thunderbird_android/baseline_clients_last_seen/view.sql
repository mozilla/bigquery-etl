-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.thunderbird_android.baseline_clients_last_seen`
AS
SELECT
  "net_thunderbird_android" AS normalized_app_id,
  * REPLACE ("release" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android.baseline_clients_last_seen`
UNION ALL
SELECT
  "net_thunderbird_android_beta" AS normalized_app_id,
  * REPLACE ("beta" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android_beta.baseline_clients_last_seen`
UNION ALL
SELECT
  "net_thunderbird_android_daily" AS normalized_app_id,
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android_daily.baseline_clients_last_seen`
