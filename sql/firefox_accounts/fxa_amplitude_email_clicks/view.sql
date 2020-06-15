CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_amplitude_email_clicks`
AS
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
sendjobs_numbered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY SendID ORDER BY snapshot_date DESC) AS _n
  FROM
    `mozilla-cdp-prod.sfmc.sendjobs`
),
sendjobs AS (
  SELECT
    * EXCEPT (_n)
  FROM
    sendjobs_numbered
  WHERE
    _n = 1
),
customers_numbered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY email ORDER BY fxa_ts DESC) AS _n
  FROM
    `mozilla-cdp-prod.fxa.customer_record`
),
customers_deduped AS (
  SELECT
    * EXCEPT (_n),
    IF(
      net.public_suffix(email) IS NULL,
      NULL,
      rtrim(
        substr(net.host(email), 1, length(net.host(email)) - length(net.public_suffix(email))),
        '.'
      )
    ) AS email_provider
  FROM
    customers_numbered
  WHERE
    _n = 1
),
-- We filter out email providers with fewer than 10k customers in this dataset,
-- roughly similar to how we filter out cities with fewer than 10k residents for
-- GeoIP lookups in the data pipeline.
email_providers_over_10k AS (
  SELECT
    email_provider
  FROM
    customers_deduped
  GROUP BY
    1
  HAVING
    count(*) > 10000
),
customers AS (
  SELECT
    * EXCEPT (email_provider),
    TO_HEX(
      `moz-fx-data-shared-prod`.udf.hmac_sha256((SELECT * FROM hmac_key), CAST(fxa_id AS BYTES))
    ) AS user_id,
    email_providers_over_10k.email_provider,
  FROM
    customers_deduped
  LEFT JOIN
    email_providers_over_10k
  USING
    (email_provider)
)
SELECT
  clicks.EventDate AS submission_timestamp,
  UNIX_MILLIS(clicks.EventDate) AS `time`,
  customers.user_id,
  ARRAY_TO_STRING([customers.user_id, clicks.SendId, string(clicks.EventDate)], '-') AS insert_id,
  FORMAT(
    '{%t}',
    ARRAY_TO_STRING(
      ARRAY(
        SELECT
          FORMAT('"%t":"%t"', key, value)
        FROM
          UNNEST(
            [
              STRUCT('email_provider' AS key, customers.email_provider AS value),
              STRUCT(
                'utm_source' AS key,
                REGEXP_EXTRACT(clicks.URL, r"[?&]utm_source=([^&]+)") AS value
              ),
              STRUCT(
                'utm_medium' AS key,
                REGEXP_EXTRACT(clicks.URL, r"[?&]utm_medium=([^&]+)") AS value
              ),
              STRUCT(
                'utm_campaign' AS key,
                REGEXP_EXTRACT(clicks.URL, r"[?&]utm_campaign=([^&]+)") AS value
              ),
              STRUCT(
                'utm_content' AS key,
                REGEXP_EXTRACT(clicks.URL, r"[?&]utm_content=([^&]+)") AS value
              ),
              STRUCT(
                'utm_term' AS key,
                REGEXP_EXTRACT(clicks.URL, r"[?&]utm_term=([^&]+)") AS value
              )
            ]
          )
        WHERE
          value IS NOT NULL
      ),
      ','
    )
  ) AS user_properties,
  FORMAT(
    '{%t}',
    ARRAY_TO_STRING(
      ARRAY(
        SELECT
          FORMAT('"%t":"%t"', key, value)
        FROM
          UNNEST(
            [
              STRUCT('service' AS key, customers.service AS value),
              STRUCT('email_alias' AS key, clicks.alias AS value),
              STRUCT('email_type' AS key, clicks.EventType AS value),
              -- sendjobs.EmailName is parsed via regex; each regex differs only
              -- in which group is doing the capturing (no ?: prefix);
              -- see original regex in https://github.com/mozilla/fxa-amplitude-send/blob/89bfaef20a1d978fce2dccddc0155699a17ac172/marketing.js#L109
              STRUCT(
                'email_sender' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"([A-Za-z]+)_(?:[A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(?:ALL|[A-Z]{2})_(?:[A-Z]{2,4})_(?:[A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_region' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_([A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(?:ALL|[A-Z]{2})_(?:[A-Z]{2,4})_(?:[A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_focus' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_(?:[A-Z]+)_([A-Z]*)_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(?:ALL|[A-Z]{2})_(?:[A-Z]{2,4})_(?:[A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_format' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_(?:[A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_([A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(?:ALL|[A-Z]{2})_(?:[A-Z]{2,4})_(?:[A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_id' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_(?:[A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(.+?)_(?:ALL|[A-Z]{2})_(?:[A-Z]{2,4})_(?:[A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_country' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_(?:[A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(ALL|[A-Z]{2})_(?:[A-Z]{2,4})_(?:[A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_language' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_(?:[A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(?:ALL|[A-Z]{2})_([A-Z]{2,4})_(?:[A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_channel' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_(?:[A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(?:ALL|[A-Z]{2})_(?:[A-Z]{2,4})_([A-Z-]+)(?:_[A-Za-z0-9]*)?"
                ) AS value
              ),
              STRUCT(
                'email_version' AS key,
                REGEXP_EXTRACT(
                  sendjobs.EmailName,
                  r"(?:[A-Za-z]+)_(?:[A-Z]+)_[A-Z]*_[0-9]{4}_[A-Z]+_(?:[A-Z]+|DESK[_ ][A-Z]+)_(?:.+?)_(?:ALL|[A-Z]{2})_(?:[A-Z]{2,4})_(?:[A-Z-]+)(?:_([A-Za-z0-9]*))?"
                ) AS value
              )
            ]
          )
        WHERE
          value IS NOT NULL
      ),
      ','
    )
  ) AS event_properties,
FROM
  `mozilla-cdp-prod.sfmc.clicks` AS clicks
LEFT JOIN
  customers
ON
  (clicks.EmailAddress = customers.email)
LEFT JOIN
  sendjobs
USING
  (SendID)
WHERE
  user_id IS NOT NULL
