WITH RECURSIVE subscription_starts AS (
  SELECT
    subscription.metadata.purchase_token,
    MIN_BY(subscription.start_time, `timestamp`) AS start_time,
    MIN_BY(subscription.linked_purchase_token, `timestamp`) AS linked_purchase_token
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_revised_changelog_v1`
  GROUP BY
    subscription.metadata.purchase_token
),
linked_purchase_tokens AS (
  SELECT DISTINCT
    purchase_token,
    linked_purchase_token
  FROM
    subscription_starts
  WHERE
    linked_purchase_token IS NOT NULL
),
recursive_linked_purchase_tokens AS (
  SELECT
    purchase_token,
    linked_purchase_token,
    1 AS link_chain_length
  FROM
    linked_purchase_tokens
  UNION ALL
  SELECT
    recursive_linked_purchase_tokens.purchase_token,
    linked_purchase_tokens.linked_purchase_token,
    recursive_linked_purchase_tokens.link_chain_length + 1 AS link_chain_length
  FROM
    recursive_linked_purchase_tokens
  JOIN
    linked_purchase_tokens
    ON recursive_linked_purchase_tokens.linked_purchase_token = linked_purchase_tokens.purchase_token
),
original_linked_purchase_tokens AS (
  SELECT
    purchase_token,
    MAX_BY(linked_purchase_token, link_chain_length) AS original_purchase_token
  FROM
    recursive_linked_purchase_tokens
  GROUP BY
    purchase_token
),
original_purchase_tokens AS (
  SELECT
    purchase_token,
    COALESCE(
      original_linked_purchase_tokens.original_purchase_token,
      purchase_token
    ) AS original_purchase_token
  FROM
    subscription_starts
  LEFT JOIN
    original_linked_purchase_tokens
    USING (purchase_token)
),
replaced_subscriptions AS (
  SELECT
    linked_purchase_token AS purchase_token,
    MIN(start_time) AS replaced_at
  FROM
    subscription_starts
  WHERE
    linked_purchase_token IS NOT NULL
  GROUP BY
    linked_purchase_token
),
subscriptions_revised_changelog AS (
  SELECT
    changelog.*,
    original_purchase_tokens.original_purchase_token AS original_subscription_purchase_token
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_revised_changelog_v1` AS changelog
  JOIN
    original_purchase_tokens
    ON changelog.subscription.metadata.purchase_token = original_purchase_tokens.purchase_token
  LEFT JOIN
    replaced_subscriptions
    ON changelog.subscription.metadata.purchase_token = replaced_subscriptions.purchase_token
  WHERE
    -- Exclude subscription changelog records where it has been replaced by a subsequent purchase,
    -- because we'll use the subsequent purchase's records for the subscription from that point on.
    changelog.subscription.metadata.replaced_by_another_purchase IS NOT TRUE
    AND changelog.subscription.cancel_reason IS DISTINCT FROM 2
    AND (
      changelog.timestamp < replaced_subscriptions.replaced_at
      OR replaced_subscriptions.replaced_at IS NULL
    )
  QUALIFY
    -- Exclude records for suspected expirations when it later turned out the subscription hadn't expired.
    (
      changelog.type = 'synthetic_subscription_suspected_expiration'
      AND LEAD(changelog.subscription.expiry_time) OVER subscription_changes_asc > (
        LEAD(changelog.timestamp) OVER subscription_changes_asc
      )
    ) IS NOT TRUE
  WINDOW
    subscription_changes_asc AS (
      PARTITION BY
        original_purchase_tokens.original_purchase_token
      ORDER BY
        changelog.timestamp,
        changelog.id
    )
)
SELECT
  CONCAT(
    original_subscription_purchase_token,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`)
  ) AS id,
  `timestamp` AS valid_from,
  COALESCE(
    LEAD(`timestamp`) OVER (
      PARTITION BY
        original_subscription_purchase_token
      ORDER BY
        `timestamp`,
        id
    ),
    '9999-12-31 23:59:59.999999'
  ) AS valid_to,
  id AS google_subscriptions_revised_changelog_id,
  original_subscription_purchase_token,
  subscription
FROM
  subscriptions_revised_changelog
