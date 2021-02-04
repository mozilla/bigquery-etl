/*

See https://bugzilla.mozilla.org/show_bug.cgi?id=1633918

Due to permissions issues, this view is published via the fxa_export_to_amplitude DAG
rather than the schema deployment pipeline.

*/
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
-- sendjobs.EmailName is parsed via regex;
-- see format definition in https://docs.google.com/spreadsheets/d/1mHpXSoDcfYK8XSX4bf-CHSfdn8XY5svDAw8ik5FhPrs/edit#gid=1626564614
-- and original regex in https://github.com/mozilla/fxa-amplitude-send/blob/89bfaef20a1d978fce2dccddc0155699a17ac172/marketing.js#L109
email_name_regex AS (
  SELECT AS VALUE
    r"^\w*(MoCo|MoFo|PKT)_([A-Z]+)_([A-Za-z-]+)_(20[0-9][0-9])(?:_([A-Za-z-]+))?_(?:HTML|TEXT)_([^_]+)(?:_(ALL|[A-Z]{2}))?_([A-Za-z-]+)_EML(?:_(.*))?"
),
-- The sendjobs table is tiny (< 1 GB) and a given sendjob can appear in
-- multiple snapshots, so we deduplicate over all history, taking the newest.
sendjobs_numbered AS (
  SELECT
    *,
    SPLIT(
      REGEXP_REPLACE(EmailName, (SELECT * FROM email_name_regex), r"\1;\2;\3;\4;\5;\6;\7;\8;\9"),
      ';'
    ) AS parsed,
    ROW_NUMBER() OVER (PARTITION BY SendID ORDER BY snapshot_date DESC) AS _n
  FROM
    `mozilla-cdp-prod.sfmc.sendjobs`
  WHERE
    REGEXP_CONTAINS(EmailName, (SELECT * FROM email_name_regex))
),
sendjobs AS (
  SELECT
    * EXCEPT (_n, parsed),
    parsed[ORDINAL(1)] AS email_sender,
    parsed[ORDINAL(2)] AS email_region,
    parsed[ORDINAL(3)] AS email_audience,
    parsed[ORDINAL(4)] AS email_year,
    parsed[ORDINAL(5)] AS email_category,
    parsed[ORDINAL(6)] AS email_id,
    parsed[ORDINAL(7)] AS email_country,
    parsed[ORDINAL(8)] AS email_language,
    parsed[ORDINAL(9)] AS email_push_number,
  FROM
    sendjobs_numbered
  WHERE
    _n = 1
),
-- Likewise, the customer_record table is fairly small (~10 GB), so it is feasible
-- to deduplicate over all history on the fly.
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
      NET.PUBLIC_SUFFIX(email) IS NULL,
      NULL,
      -- We construct an "email_provider" field that is a simple name like "gmail", "outlook", etc.
      -- We use some BQ NET functions and string munging to get the hostname and
      -- remove common suffixes ".com", ".net", ".co.uk", etc.
      RTRIM(
        SUBSTR(NET.HOST(email), 1, LENGTH(NET.HOST(email)) - LENGTH(NET.PUBLIC_SUFFIX(email))),
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
  TIMESTAMP(clicks.snapshot_date) AS submission_timestamp,
  UNIX_MILLIS(clicks.EventDate) AS `time`,
  customers.user_id,
  ARRAY_TO_STRING([customers.user_id, clicks.SendId, string(clicks.EventDate)], '-') AS insert_id,
  'mktg - email_click' AS event_type,
  FORMAT(
    -- We use CONCAT here to avoid '{' directly followed by '%' which will be
    -- interpreted as opening a Jinja statement when run via Airflow's BigQueryOperator.
    CONCAT('{', '%t', '}'),
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
    CONCAT('{', '%t', '}'),
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
              STRUCT('email_sender' AS key, email_sender AS value),
              STRUCT('email_region' AS key, email_region AS value),
              STRUCT('email_audience' AS key, email_audience AS value),
              STRUCT('email_year' AS key, email_year AS value),
              STRUCT('email_category' AS key, email_category AS value),
              STRUCT('email_id' AS key, email_id AS value),
              STRUCT('email_country' AS key, email_country AS value),
              STRUCT('email_language' AS key, email_language AS value),
              STRUCT('email_push_number' AS key, email_push_number AS value)
            ]
          )
        WHERE
          value IS NOT NULL
          AND value != ''
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
