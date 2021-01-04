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
  TO_HEX(
    udf.hmac_sha256((SELECT * FROM hmac_key), CAST(jsonPayload.fields.user_id AS BYTES))
  ) AS user_id
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth_20*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%y%m%d', @submission_date)
UNION DISTINCT
SELECT
  user_id
FROM
  fxa_amplitude_user_ids_v1
