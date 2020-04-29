WITH base_events
AS
  (
    SELECT
      *
    FROM
      `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth_20*`
    WHERE
      _TABLE_SUFFIX = FORMAT_DATE('%g%m%d', @submission_date)
      AND jsonPayload.fields.event_type IN (
        'fxa_activity - cert_signed',
        'fxa_activity - access_token_checked',
        'fxa_activity - access_token_created'
      )
      AND jsonPayload.fields.user_id IS NOT NULL
  ),
  grouped_by_user AS (
    SELECT
    -- to prevent weirdness from timestamp field, use provided
    -- submission date parameter as timestamp
      CAST(@submission_date AS DATETIME) AS submission_timestamp,
      TO_HEX(
        udf.hmac_sha256(CAST(@fxa_hmac AS BYTES), CAST(jsonPayload.fields.user_id AS BYTES))
      ) AS user_id,
      MIN(CONCAT(insertId, '-user')) AS insert_id,
      CAST(@submission_date AS DATETIME) AS timestamp,
    -- Amplitude properties, scalars
      `moz-fx-data-shared-prod`.udf.mode_last(ARRAY_AGG(jsonPayload.fields.region)) AS region,
      `moz-fx-data-shared-prod`.udf.mode_last(ARRAY_AGG(jsonPayload.fields.country)) AS country,
      `moz-fx-data-shared-prod`.udf.mode_last(
        ARRAY_AGG(jsonPayload.fields.`language`)
      ) AS `language`,
    -- Event properties, arrays
      ARRAY_AGG(
        DISTINCT JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, "$.service") IGNORE NULLS
      ) AS services,
      ARRAY_AGG(
        DISTINCT JSON_EXTRACT_SCALAR(
          jsonPayload.fields.event_properties,
          "$.oauth_client_id"
        ) IGNORE NULLS
      ) AS oauth_client_ids,
    -- User properties, arrays
      ARRAY_AGG(
        DISTINCT JSON_EXTRACT_SCALAR(
          jsonPayload.fields.user_properties,
          "$['$append'].fxa_services_used"
        ) IGNORE NULLS
      ) AS fxa_services_used,
      ARRAY_AGG(DISTINCT jsonPayload.fields.os_name IGNORE NULLS) AS os_used_day,
    -- User properties, scalars
      MAX(
        JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.sync_device_count")
      ) AS sync_device_count,
      MAX(
        JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.sync_active_devices_day")
      ) AS sync_active_devices_day,
      MAX(
        JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.sync_active_devices_week")
      ) AS sync_active_devices_week,
      MAX(
        JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.sync_active_devices_month")
      ) AS sync_active_devices_month,
      `moz-fx-data-shared-prod`.udf.mode_last(
        ARRAY_AGG(
          JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.ua_version") IGNORE NULLS
        )
      ) AS ua_version,
      `moz-fx-data-shared-prod`.udf.mode_last(
        ARRAY_AGG(
          JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.ua_version") IGNORE NULLS
        )
      ) AS ua_browser,
      MAX(jsonPayload.fields.app_version) AS app_version,
    FROM
      base_events
    GROUP BY
      user_id
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
      grouped_by_user
  ),
  user_properties AS (
    SELECT
      submission_timestamp,
      user_id,
      '' AS insert_id,
      '$identify' AS event_type,
      timestamp,
      '' AS event_properties,
    -- $ is not valid for a column name, so edit it into the json string
      REPLACE(
        TO_JSON_STRING(
          STRUCT(
            region,
            country,
            LANGUAGE,
            os_used_day,
            sync_device_count,
            sync_active_devices_day,
            sync_active_devices_week,
            sync_active_devices_month,
            ua_version,
            ua_browser,
            app_version,
            STRUCT(fxa_services_used) AS str_dollar_sign_postInsert
          )
        ),
        'str_dollar_sign_',
        '$'
      )
    FROM
      grouped_by_user
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
