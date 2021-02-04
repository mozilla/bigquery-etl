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
)
SELECT
  `timestamp` AS submission_timestamp,
  TO_HEX(SHA256(jsonPayload.fields.uid)) AS user_id,
  TO_HEX(
    udf.hmac_sha256((SELECT * FROM hmac_key), CAST(jsonPayload.fields.uid AS BYTES))
  ) AS hmac_user_id,
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth_20*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%y%m%d', @submission_date)
  AND jsonPayload.type = 'activityEvent'
  AND jsonPayload.fields.event = 'account.deleted'
  AND jsonPayload.fields.uid IS NOT NULL
