-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.net_thunderbird_android_daily.events_stream`
AS
SELECT
  *,
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android_daily_derived.events_stream_v1`
