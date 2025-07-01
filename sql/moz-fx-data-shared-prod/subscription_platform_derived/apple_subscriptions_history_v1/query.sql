WITH subscriptions_revised_changelog AS (
  SELECT
    id,
    `timestamp`,
    subscription
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_revised_changelog_v1`
  -- Exclude records for suspected expirations and billing retry periods when it later turned out
  -- the subscription hadn't expired or entered a billing retry period.
  QUALIFY
    (
      LEAD(subscription.status) OVER subscription_changes_asc IN (
        1,  -- 1 = active
        4   -- 4 = in billing grace period
      )
      AND (
        (
          type IN (
            'synthetic_suspected_subscription_expiration',
            'synthetic_suspected_billing_retry_period_start'
          )
          AND (
            LEAD(subscription.last_transaction.purchase_date) OVER subscription_changes_asc
          ) <= subscription.last_transaction.expires_date
        )
        OR (
          type = 'synthetic_suspected_billing_retry_period_expiration'
          AND (
            LEAD(subscription.last_transaction.purchase_date) OVER subscription_changes_asc
          ) <= `timestamp`
        )
      )
    ) IS NOT TRUE
  WINDOW
    subscription_changes_asc AS (
      PARTITION BY
        subscription.original_transaction_id
      ORDER BY
        `timestamp`,
        id
    )
),
synthetic_subscription_period_start_changelog AS (
  -- Synthesize changelog records representing the exact start of each new subscription period when possible.
  -- This is also done in `apple_subscriptions_revised_changelog_v1`, but there the synthetic expiration and
  -- billing retry period changelog records could block that from happening, so we do it again here where
  -- those synthetic expiration and billing retry period changelog records have potentially been excluded.
  SELECT
    id,
    subscription.last_transaction.purchase_date AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          1 AS status,  -- 1 = active
          (
            SELECT AS STRUCT
              subscription.renewal_info.* REPLACE (
                CAST(NULL AS INTEGER) AS expiration_intent,
                CAST(NULL AS TIMESTAMP) AS grace_period_expires_date
              )
          ) AS renewal_info,
          (
            SELECT AS STRUCT
              subscription.metadata.* REPLACE (
                CAST(NULL AS STRING) AS latest_notification_type,
                CAST(NULL AS STRING) AS latest_notification_subtype,
                CAST(NULL AS TIMESTAMP) AS verified_at
              )
          ) AS metadata
        )
    ) AS subscription
  FROM
    subscriptions_revised_changelog
  QUALIFY
    subscription.last_transaction.purchase_date IS DISTINCT FROM (
      LAG(subscription.last_transaction.purchase_date) OVER subscription_changes_asc
    )
    AND subscription.last_transaction.purchase_date < `timestamp`
    AND subscription.last_transaction.purchase_date > COALESCE(
      LAG(`timestamp`) OVER subscription_changes_asc,
      '0001-01-01 00:00:00'
    )
    AND subscription.status IN (
      1,  -- 1 = active
      4   -- 4 = in billing grace period
    )
  WINDOW
    subscription_changes_asc AS (
      PARTITION BY
        subscription.original_transaction_id
      ORDER BY
        `timestamp`,
        id
    )
),
changelog_union AS (
  SELECT
    id,
    `timestamp`,
    subscription
  FROM
    subscriptions_revised_changelog
  UNION ALL
  SELECT
    id,
    `timestamp`,
    subscription
  FROM
    synthetic_subscription_period_start_changelog
)
SELECT
  CONCAT(
    subscription.original_transaction_id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`)
  ) AS id,
  `timestamp` AS valid_from,
  COALESCE(
    LEAD(`timestamp`) OVER (
      PARTITION BY
        subscription.original_transaction_id
      ORDER BY
        `timestamp`,
        id
    ),
    '9999-12-31 23:59:59.999999'
  ) AS valid_to,
  id AS apple_subscriptions_revised_changelog_id,
  subscription
FROM
  changelog_union
