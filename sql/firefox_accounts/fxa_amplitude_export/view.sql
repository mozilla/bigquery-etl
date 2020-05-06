CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_amplitude_export`
AS
WITH active_users AS (
  SELECT
    `moz-fx-data-shared-prod`.udf.active_values_from_days_seen_map(os_used_month, 0, 1) AS os_used_day,
    `moz-fx-data-shared-prod`.udf.active_values_from_days_seen_map(os_used_month, -6, 7) AS os_used_week,
    `moz-fx-data-shared-prod`.udf.active_values_from_days_seen_map(os_used_month, -27, 28) AS os_used_month,
    * EXCEPT (days_seen_bits, os_used_month)
  FROM
    `moz-fx-data-shared-prod`.firefox_accounts_derived.fxa_amplitude_export_v1
  WHERE
    `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(days_seen_bits) = 0
),
active_events AS (
  SELECT
    submission_timestamp,
    user_id,
    insert_id,
    'fxa_activity - active' AS event_type,
    timestamp,
    TO_JSON_STRING(STRUCT(services, oauth_client_ids)) AS event_properties,
    '' AS user_events
  FROM
    active_users
),
user_properties AS (
  SELECT
    submission_timestamp,
    user_id,
    '' AS insert_id,
    '$identify' AS event_type,
    timestamp,
    '' AS event_properties,
    -- We don't want to include user_properties if they are null, so we need
    -- to list them out explicitly and filter with WHERE
    CONCAT(
      "{",
      ARRAY_TO_STRING(
        ARRAY(
          SELECT
            CONCAT(TO_JSON_STRING(key), ":", value)
          FROM
            (
              SELECT AS STRUCT
                "region" AS key,
                TO_JSON_STRING(region) AS value,
              UNION ALL
              SELECT AS STRUCT
                "country" AS key,
                TO_JSON_STRING(country) AS value,
              UNION ALL
              SELECT AS STRUCT
                "LANGUAGE" AS key,
                TO_JSON_STRING(LANGUAGE) AS value,
              UNION ALL
              SELECT AS STRUCT
                "os_used_day" AS key,
                TO_JSON_STRING(os_used_day) AS value,
              UNION ALL
              SELECT AS STRUCT
                "os_used_week" AS key,
                TO_JSON_STRING(os_used_week) AS value,
              UNION ALL
              SELECT AS STRUCT
                "os_used_month" AS key,
                TO_JSON_STRING(os_used_month) AS value,
              UNION ALL
              SELECT AS STRUCT
                "sync_device_count" AS key,
                TO_JSON_STRING(sync_device_count) AS value,
              UNION ALL
              SELECT AS STRUCT
                "sync_active_devices_day" AS key,
                TO_JSON_STRING(sync_active_devices_day) AS value,
              UNION ALL
              SELECT AS STRUCT
                "sync_active_devices_week" AS key,
                TO_JSON_STRING(sync_active_devices_week) AS value,
              UNION ALL
              SELECT AS STRUCT
                "sync_active_devices_month" AS key,
                TO_JSON_STRING(sync_active_devices_month) AS value,
              UNION ALL
              SELECT AS STRUCT
                "ua_version" AS key,
                TO_JSON_STRING(ua_version) AS value,
              UNION ALL
              SELECT AS STRUCT
                "ua_browser" AS key,
                TO_JSON_STRING(ua_browser) AS value,
              UNION ALL
              SELECT AS STRUCT
                "app_version" AS key,
                TO_JSON_STRING(app_version) AS value,
              UNION ALL
              SELECT AS STRUCT
                "$postInsert",
                TO_JSON_STRING(STRUCT(fxa_services_used)) AS value
            )
          WHERE
            value != "null"
        ),
        ","
      ),
      "}"
    ) AS used_properties
  FROM
    active_users
),
all_events AS (
  SELECT
    *
  FROM
    active_events
  UNION ALL
  SELECT
    *
  FROM
    user_properties
)
SELECT
  *
FROM
  all_events
