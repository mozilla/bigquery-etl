-- Function to coerce a raw value `v` to the nearest quantile to make fingerprinting
-- more difficult. The method here attempts to keep aggregates fairly close to aggregates
-- over raw values by bucketing based on pairs of quantiles, and using the lower
-- quantile as a rough midpoint for each bucket. When adding a new metric, you'll need
-- to pass in a quantile array. See the following example query:
/*

-- Sample query for generating a quantile list for a new metric.
SELECT
  FORMAT("%T", APPROX_QUANTILES(payload.duration, 10 ignore nulls)),
FROM
  `moz-fx-data-shared-prod.telemetry_stable.account_ecosystem_v4`
WHERE
  DATE(submission_timestamp) BETWEEN "2020-09-01" AND "2020-10-01"

*/
CREATE TEMP FUNCTION quantilify(v ANY TYPE, quantiles ARRAY<INT64>) AS (
  (
    WITH boundaries AS (
      SELECT
        ARRAY(
          SELECT
            n
          FROM
            UNNEST(quantiles) AS n
            WITH OFFSET AS i
          WHERE
            mod(i, 2) = 0
            AND i
            BETWEEN 1
            AND ARRAY_LENGTH(quantiles) - 2
        ) AS uppers,
        ARRAY(
          SELECT
            n
          FROM
            UNNEST(quantiles) AS n
            WITH OFFSET AS i
          WHERE
            MOD(i, 2) = 1
        ) AS midpoints,
    )
    SELECT
      midpoints[OFFSET(RANGE_BUCKET(v, uppers))]
    FROM
      boundaries
  )
);

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
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  TO_HEX(
    udf.hmac_sha256((SELECT * FROM hmac_key), CAST(payload.ecosystem_client_id AS BYTES))
  ) AS ecosystem_client_id_hash,
  SUM(
    quantilify(
      payload.duration,
      [1, 94, 486, 1934, 5316, 12699, 26003, 41084, 80694, 86400, 2717662]
    )
  ) AS duration_sum,
  SUM(
    quantilify(
      payload.scalars.parent.browser_engagement_active_ticks,
      [1, 3, 12, 29, 60, 118, 212, 395, 699, 1256, 6649]
    ) / (3600 / 5)
  ) AS active_hours_sum,
  SUM(
    quantilify(
      payload.scalars.parent.browser_engagement_total_uri_count,
      [1, 3, 7, 13, 22, 39, 71, 126, 227, 432, 37284]
    )
  ) AS scalar_parent_browser_engagement_total_uri_count_sum,
  SUM(payload.scalars.parent.browser_engagement_total_uri_count) >= 5 AS visited_5_uri,
  SUM(payload.scalars.parent.browser_engagement_total_uri_count) >= 10 AS visited_10_uri,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_channel)) AS normalized_channel,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os)) AS normalized_os,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_country_code)) AS normalized_country_code,
FROM
  telemetry.account_ecosystem
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  payload.ecosystem_client_id
