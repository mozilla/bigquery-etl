-- Query for fenix_derived.credit_card_usage_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
-- Query for fenix_derived.feature_usage_credit_card_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH dau_segments AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    100 * COUNT(DISTINCT client_info.client_id) AS dau
  FROM
    `moz-fx-data-shared-prod.fenix.metrics`
--AND channel = 'release'
  WHERE
    DATE(submission_timestamp) >= @submission_date
    AND sample_id = 0
  GROUP BY
    1
),
product_features AS (
  SELECT
    client_info.client_id,
    DATE(submission_timestamp) AS submission_date,
    COALESCE(SUM(metrics.counter.credit_cards_deleted), 0) AS credit_cards_deleted,
    COALESCE(SUM(metrics.quantity.credit_cards_saved_all), 0) AS currently_stored_credit_cards,
  FROM
    `moz-fx-data-shared-prod.fenix.metrics`
  WHERE
    DATE(submission_timestamp) >= @submission_date
    AND sample_id = 0
  GROUP BY
    1,
    2
),
product_features_agg AS (
  SELECT
    submission_date,
    --credit card deleted
    100 * COUNT(
      DISTINCT
      CASE
        WHEN credit_cards_deleted > 0
          THEN client_id
      END
    ) AS credit_cards_deleted_users,
    100 * COALESCE(SUM(credit_cards_deleted), 0) AS credit_cards_deleted,
    --credit card currently stored
    100 * COUNT(
      DISTINCT
      CASE
        WHEN currently_stored_credit_cards > 0
          THEN client_id
      END
    ) AS currently_stored_credit_cards_users,
    100 * COALESCE(SUM(currently_stored_credit_cards), 0) AS currently_stored_credit_cards
  FROM
    product_features
  WHERE
    submission_date >= '2022-01-01'
  GROUP BY
    1
)
SELECT
  d.submission_date
-- credit card deleted
  ,
  SAFE_DIVIDE(p.credit_cards_deleted, p.credit_cards_deleted_users) AS credit_cards_deleted_avg,
  p.credit_cards_deleted_users,
  100 * SAFE_DIVIDE(p.credit_cards_deleted_users, dau) AS credit_cards_deleted_frac
-- credit card currently stored
  ,
  SAFE_DIVIDE(
    p.currently_stored_credit_cards,
    currently_stored_credit_cards_users
  ) AS currently_stored_credit_cards_avg,
  currently_stored_credit_cards_users,
  100 * SAFE_DIVIDE(currently_stored_credit_cards_users, dau) AS currently_stored_credit_cards_frac
FROM
  dau_segments d
LEFT JOIN
  product_features_agg p
ON
  d.submission_date = p.submission_date
