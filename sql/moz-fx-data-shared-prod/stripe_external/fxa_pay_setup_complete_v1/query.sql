WITH fxa_events AS (
  SELECT
    jsonPayload.fields.event_type,
    `timestamp` AS event_timestamp,
    TO_HEX(SHA256(jsonPayload.fields.user_id)) AS fxa_uid,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, "$.plan_id") AS plan_id,
    JSON_EXTRACT_SCALAR(
      jsonPayload.fields.event_properties,
      "$.payment_provider"
    ) AS payment_provider,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, "$.source_country") AS source_country,
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.stdout_*` AS event
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d", @date)
)
SELECT
  * EXCEPT (event_type)
FROM
  fxa_events
WHERE
  event_type = "fxa_pay_setup - 3ds_complete"
  AND fxa_uid IS NOT NULL
  AND plan_id IS NOT NULL
  AND source_country IS NOT NULL
