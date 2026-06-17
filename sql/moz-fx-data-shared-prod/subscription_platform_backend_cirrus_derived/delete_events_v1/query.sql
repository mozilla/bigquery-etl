CREATE TEMP FUNCTION format_uuid(value STRING)
RETURNS STRING AS (
  CONCAT(
    SUBSTR(value, 1, 8),
    "-",
    SUBSTR(value, 9, 4),
    "-",
    SUBSTR(value, 13, 4),
    "-",
    SUBSTR(value, 17, 4),
    "-",
    SUBSTR(value, 21, 12)
  )
);

CREATE TEMP FUNCTION uuid_v5(value STRING, namespace STRING)
RETURNS STRING AS (
  FORMAT_UUID(
    TO_HEX(
      LEFT(
        SHA1(CONCAT(FROM_HEX(REPLACE(namespace, "-", "")), CAST(value AS BYTES FORMAT 'UTF8'))),
        16
      ) --
      & FROM_HEX('ffffffffffff0fff3fffffffffffffff') --
      | FROM_HEX('00000000000050008000000000000000')
    )
  )
);

WITH subplat_namespaces AS (
  SELECT
    CAST(
      AEAD.DECRYPT_BYTES(
        (SELECT keyset FROM `moz-fx-dataops-secrets.airflow_query_keys.subplat_namespaces_prod`),
        ciphertext,
        CAST(key_id AS BYTES)
      ) AS STRING
    ) AS namespace
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.encrypted_keys_v1`
  WHERE
    key_id = 'subplat_namespaces_prod'
)
SELECT
  fxa_delete_events.submission_timestamp,
  uuid_v5(fxa_delete_events.user_id_unhashed, subplat_namespaces.namespace) AS nimbus_user_id,
FROM
  `moz-fx-data-shared-prod.firefox_accounts.fxa_delete_events` AS fxa_delete_events
CROSS JOIN
  subplat_namespaces
WHERE
  DATE(fxa_delete_events.submission_timestamp) = @submission_date
  AND fxa_delete_events.user_id_unhashed IS NOT NULL
