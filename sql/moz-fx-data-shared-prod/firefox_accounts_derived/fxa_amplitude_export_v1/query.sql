WITH hmac_key AS (
  SELECT
    AEAD.DECRYPT_BYTES(
      (SELECT keyset FROM `moz-fx-dataops-secrets.airflow_query_keys.fxa_prod`),
      ciphertext,
      CAST(key_id AS BYTES)
    ) AS value
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.encrypted_keys_v1`
  WHERE
    key_id = 'fxa_hmac_prod'
),
base_events AS (
  SELECT
    *
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth`
  WHERE
    -- @submission_date is PDT, so we need two days of UTC-based
    -- data. We assume that we run immediately at the end of
    -- @submission_date, so we need data from the _next_ UTC-based
    -- submission date.
    -- See https://console.cloud.google.com/bigquery?sq=768515352537:e63d2d2faa85431dbf0e5440021af837
    (
      DATE(`timestamp`) = @submission_date
      OR DATE(`timestamp`) = DATE_ADD(@submission_date, INTERVAL 1 DAY)
    )
    AND DATE(`timestamp`, "America/Los_Angeles") = @submission_date
    AND jsonPayload.fields.event_type IN (
      'fxa_activity - cert_signed',
      'fxa_activity - access_token_checked',
      'fxa_activity - access_token_created',
      'fxa_activity - oauth_access_token_created'
    )
    AND jsonPayload.fields.user_id IS NOT NULL
),
grouped_by_user AS (
  SELECT
    -- to prevent weirdness from timestamp field, use provided
    -- submission date parameter as timestamp
    TO_HEX(
      udf.hmac_sha256((SELECT * FROM hmac_key), CAST(jsonPayload.fields.user_id AS BYTES))
    ) AS user_id,
    MIN(CONCAT(insertId, '-user')) AS insert_id,
    -- Amplitude properties, scalars
    `moz-fx-data-shared-prod`.udf.mode_last(ARRAY_AGG(jsonPayload.fields.region)) AS region,
    `moz-fx-data-shared-prod`.udf.mode_last(ARRAY_AGG(jsonPayload.fields.country)) AS country,
    `moz-fx-data-shared-prod`.udf.mode_last(ARRAY_AGG(jsonPayload.fields.`language`)) AS `language`,
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
    ARRAY_AGG(DISTINCT jsonPayload.fields.os_name IGNORE NULLS) AS os_used_month,
    -- User properties, scalars
    MAX(
      CAST(JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.sync_device_count") AS INT64)
    ) AS sync_device_count,
    MAX(
      CAST(
        JSON_EXTRACT_SCALAR(
          jsonPayload.fields.user_properties,
          "$.sync_active_devices_day"
        ) AS INT64
      )
    ) AS sync_active_devices_day,
    MAX(
      CAST(
        JSON_EXTRACT_SCALAR(
          jsonPayload.fields.user_properties,
          "$.sync_active_devices_week"
        ) AS INT64
      )
    ) AS sync_active_devices_week,
    MAX(
      CAST(
        JSON_EXTRACT_SCALAR(
          jsonPayload.fields.user_properties,
          "$.sync_active_devices_month"
        ) AS INT64
      )
    ) AS sync_active_devices_month,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(
        JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.ua_version") IGNORE NULLS
      )
    ) AS ua_version,
    `moz-fx-data-shared-prod`.udf.mode_last(
      ARRAY_AGG(
        JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.ua_browser") IGNORE NULLS
      )
    ) AS ua_browser,
    MAX(CAST(jsonPayload.fields.app_version AS FLOAT64)) AS app_version,
    CAST(TRUE AS INT64) AS days_seen_bits,
    ARRAY_AGG(DISTINCT jsonPayload.fields.event_type) AS rollup_events
  FROM
    base_events
  GROUP BY
    user_id
),
_previous AS (
  SELECT
    * EXCEPT (submission_date_pacific)
  FROM
    firefox_accounts_derived.fxa_amplitude_export_v1
  WHERE
    submission_date_pacific = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
)
SELECT
  @submission_date AS submission_date_pacific,
  _current.* REPLACE (
    COALESCE(_current.user_id, _previous.user_id) AS user_id,
    udf.combine_adjacent_days_28_bits(
      _previous.days_seen_bits,
      _current.days_seen_bits
    ) AS days_seen_bits,
    udf.combine_days_seen_maps(
      _previous.os_used_month,
      ARRAY(SELECT STRUCT(key, CAST(TRUE AS INT64) AS value) FROM _current.os_used_month AS key)
    ) AS os_used_month
  )
FROM
  grouped_by_user _current
FULL OUTER JOIN
  _previous
  USING (user_id)
