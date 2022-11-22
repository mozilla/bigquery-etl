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
--
SELECT
  `timestamp`,
  jsonPayload.type,
  TO_HEX(
    udf.hmac_sha256((SELECT * FROM hmac_key), CAST(jsonPayload.fields.uid AS BYTES))
  ) AS user_id,
  TO_HEX(
    udf.hmac_sha256(
      (SELECT * FROM hmac_key),
      CAST(FORMAT('%d', CAST(jsonPayload.fields.index AS INT64)) AS BYTES)
    )
  ) AS index,
  jsonPayload.fields.command,
  TO_HEX(
    udf.hmac_sha256((SELECT * FROM hmac_key), CAST(jsonPayload.fields.target AS BYTES))
  ) AS target,
  jsonPayload.fields.targetOS AS target_os,
  jsonPayload.fields.targetType AS target_type,
  TO_HEX(
    udf.hmac_sha256((SELECT * FROM hmac_key), CAST(jsonPayload.fields.sender AS BYTES))
  ) AS sender,
  jsonPayload.fields.senderOS AS sender_os,
  jsonPayload.fields.senderType AS sender_type,
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth`
WHERE
  jsonPayload.type LIKE 'device.command.%'
  -- Device command metrics were first deployed and stable on 2020-07-08;
  -- there is some data for earlier dates but it's from a failed deployment so we don't count it.
  AND DATE(`timestamp`) >= "2020-07-08"
  AND DATE(`timestamp`) = @submission_date
