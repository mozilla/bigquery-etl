WITH hmac_key AS (
  SELECT
    AEAD.DECRYPT_BYTES(
      (SELECT keyset FROM `moz-fx-dataops-secrets.airflow_query_keys.aet_prod`),
      ciphertext,
      CAST(key_id AS BYTES)
    ) AS value
  FROM
    `moz-fx-data-shared-prod.account_ecosystem_restricted.encrypted_keys_v1`
  WHERE
    key_id = 'aet_hmac_prod'
),
unioned AS (
  SELECT
    submission_timestamp,
    payload.scalars.parent.deletion_request_ecosystem_client_id AS ecosystem_client_id,
  FROM
    telemetry_stable.deletion_request_v4
  -- As we add AET to additional applications, they will be added here via UNION ALL
  -- and also added to the list of referenced tables in metadata.yaml
)
SELECT
  submission_timestamp,
  ecosystem_client_id,
  TO_HEX(
    udf.hmac_sha256((SELECT * FROM hmac_key), CAST(ecosystem_client_id AS BYTES))
  ) AS ecosystem_client_id_hash,
FROM
  unioned
WHERE
  ecosystem_client_id IS NOT NULL
  AND DATE(submission_timestamp) = @submission_date
