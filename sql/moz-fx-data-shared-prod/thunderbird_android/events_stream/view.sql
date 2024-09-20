-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.thunderbird_android.events_stream`
AS
SELECT
  "net_thunderbird_android" AS normalized_app_id,
  e.* REPLACE ("release" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android.events_stream` AS e
UNION ALL
SELECT
  "net_thunderbird_android_beta" AS normalized_app_id,
  e.* REPLACE ("beta" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android_beta.events_stream` AS e
UNION ALL
SELECT
  "net_thunderbird_android_daily" AS normalized_app_id,
  e.* REPLACE ("nightly" AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android_daily.events_stream` AS e
