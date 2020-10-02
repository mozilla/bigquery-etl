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
    payload.ecosystem_user_id,
    payload.ecosystem_client_id,
  FROM
    telemetry_stable.account_ecosystem_v4
  -- As we add AET to additional applications, they will be added here via UNION ALL
  -- and also added to the list of referenced tables in metadata.yaml
),
hashed AS (
  SELECT
    DISTINCT TO_HEX(
      udf.hmac_sha256((SELECT * FROM hmac_key), CAST(ecosystem_client_id AS BYTES))
    ) AS ecosystem_client_id_hash,
    ecosystem_user_id,
    DATE(submission_timestamp) AS first_seen_date,
  FROM
    unioned
  WHERE
    DATE(submission_timestamp) = @submission_date
),
euil AS (
  SELECT
    ecosystem_user_id,
    canonical_id
  FROM
    ecosystem_user_id_lookup_v1
  WHERE
    first_seen_date <= @submission_date
),
existing AS (
  SELECT
    ecosystem_client_id_hash
  FROM
    ecosystem_client_id_lookup_v1
  WHERE
    first_seen_date < @submission_date
)
SELECT
  ecosystem_client_id_hash,
  euil.canonical_id,
  hashed.first_seen_date,
FROM
  hashed
LEFT JOIN
  euil
USING
  (ecosystem_user_id)
LEFT JOIN
  existing
USING
  (ecosystem_client_id_hash)
WHERE
  existing.ecosystem_client_id_hash IS NULL
