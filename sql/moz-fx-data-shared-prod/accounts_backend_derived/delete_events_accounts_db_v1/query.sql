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
  deletedAt AS submission_timestamp,
  TO_HEX(SHA256(uid)) AS user_id,
  TO_HEX(
    `moz-fx-data-shared-prod.udf.hmac_sha256`((SELECT * FROM hmac_key), CAST(uid AS BYTES))
  ) AS hmac_user_id,
  uid AS user_id_unhashed,
FROM
  `moz-fx-data-shared-prod.accounts_db_external.fxa_deleted_accounts_v1`
WHERE
  DATE(deletedAt) = @submission_date
  AND uid IS NOT NULL
