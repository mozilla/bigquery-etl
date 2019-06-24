
--
CREATE OR REPLACE VIEW
`moz-fx-data-derived-datasets.telemetry.lockwise_mobile_events_v1` AS
  --
WITH
  android AS (
    SELECT
        os,
        client_id,
        submission_date_s3,
        timestamp_seconds(cast(substr(cast(metadata.timestamp AS string), 1, 10) AS int64)) AS submission_timestamp,
        device,
        metadata.app_build_id,
        e.element.timestamp,
        e.element.category,
        e.element.method,
        e.element.object,
        e.element.value,
        e.element.extra.key_value
    FROM `moz-fx-data-derived-datasets.telemetry.telemetry_mobile_event_parquet_v2`, UNNEST(events.list) AS e
    WHERE metadata.app_name = 'Lockbox'
    AND os = 'Android'
    -- Filter out test data before the Android launch date.
    AND submission_date_s3 >= '2019-03-25'
  ),
  --
  ios AS (
    SELECT
        os,
        client_id,
        submission_date_s3,
        timestamp_seconds(cast(substr(cast(metadata.timestamp AS string), 1, 10) AS int64)) AS submission_timestamp,
        device,
        metadata.app_build_id,
        e.element.timestamp,
        e.element.category,
        e.element.method,
        e.element.object,
        e.element.value,
        e.element.extra.key_value
    FROM
    `moz-fx-data-derived-datasets.telemetry.telemetry_focus_event_parquet_v1`, UNNEST(events.list) AS e
    WHERE metadata.app_name = 'Lockbox'
    AND os = 'iOS'
    -- Filter out test data before the iOS launch date.
    AND submission_date_s3 >= '2018-07-09'
  )
  --
SELECT * FROM android
UNION ALL
SELECT * FROM ios
