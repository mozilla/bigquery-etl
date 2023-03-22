-- Query for org_mozilla_firefox_derived.client_deduplication_v1
WITH hmac_key AS (
  SELECT
    AEAD.DECRYPT_BYTES(
      (SELECT keyset FROM `moz-fx-dataops-secrets.airflow_query_keys.ad_id_prod`),
      ciphertext,
      CAST(key_id AS BYTES)
    ) AS value
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_derived.encrypted_keys_v1`
  WHERE
    key_id = 'ad_id_prod'
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  "org_mozilla_firefox" AS normalized_app_id,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  (
    SELECT AS STRUCT
      metrics.* REPLACE (
        (
          SELECT AS STRUCT
            metrics.string.* REPLACE (
              `moz-fx-data-shared-prod`.udf.pseudonymize_ad_id(
                metrics.string.activation_identifier,
                (SELECT * FROM hmac_key)
              ) AS activation_identifier,
              `moz-fx-data-shared-prod`.udf.pseudonymize_ad_id(
                metrics.string.client_deduplication_hashed_gaid,
                (SELECT * FROM hmac_key)
              ) AS client_deduplication_hashed_gaid
            )
        ) AS string
      )
  ) AS metrics,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_stable.client_deduplication_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
