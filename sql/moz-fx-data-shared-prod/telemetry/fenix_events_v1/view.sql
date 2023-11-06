CREATE OR REPLACE VIEW
    `moz-fx-data-shared-prod.telemetry.fenix_events_v1` AS
SELECT
    submission_timestamp,
    client_info.client_id AS device_id,
    CONCAT(document_id, CAST(event.timestamp AS STRING)) AS insert_id,
    CONCAT(event.category, '.', event.name) AS event_type,
    TIMESTAMP_ADD(mozfun.glean.parse_datetime(ping_info.start_time), INTERVAL event.timestamp SECOND) AS timestamp,
    client_info.app_display_version AS app_version,
    client_info.os AS platform,
    client_info.os AS os_name,
    client_info.os_version AS os_version,
    client_info.device_manufacturer AS device_manufacturer,
    client_info.device_model AS device_model,
    metadata.geo.country AS country,
    metadata.geo.subdivision1 AS region,
    metadata.geo.city AS city,
    ( -- direct insert of `udf.kv_array_to_json_string`, for use in a view
      SELECT CONCAT(
        '{',
        ARRAY_TO_STRING(
            ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')),
            ","),
        '}')
      FROM
        UNNEST(event.extra)
    ) AS event_properties,
    TO_JSON_STRING(
      STRUCT(client_info.architecture AS arch)
    ) AS user_properties
FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.events_v1`
CROSS JOIN
    UNNEST(events) AS event
