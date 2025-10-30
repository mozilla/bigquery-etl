-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.thunderbird_android.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "net_thunderbird_android" AS normalized_app_id,
    e.* REPLACE ("release" AS normalized_channel),
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_derived.events_stream_v1` AS e
  UNION ALL
    BY NAME
  SELECT
    "net_thunderbird_android_beta" AS normalized_app_id,
    e.* REPLACE ("beta" AS normalized_channel),
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_beta_derived.events_stream_v1` AS e
  UNION ALL
    BY NAME
  SELECT
    "net_thunderbird_android_daily" AS normalized_app_id,
    e.* REPLACE ("nightly" AS normalized_channel),
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_daily_derived.events_stream_v1` AS e
)
SELECT
  *,
FROM
  events_stream_union
