WITH subscriptions_history AS (
  SELECT
    id,
    `timestamp` AS valid_from,
    COALESCE(
      LEAD(`timestamp`) OVER (PARTITION BY subscription.id ORDER BY `timestamp`),
      '9999-12-31 23:59:59.999999'
    ) AS valid_to,
    id AS stripe_subscriptions_revised_changelog_id,
    subscription
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_subscriptions_revised_changelog_v1
)
-- Include customers as they existed at the start of each subscription history period.
SELECT
  subscriptions_history.id,
  subscriptions_history.valid_from,
  IF(
    customers_history.valid_to IS NOT NULL,
    LEAST(subscriptions_history.valid_to, customers_history.valid_to),
    subscriptions_history.valid_to
  ) AS valid_to,
  subscriptions_history.stripe_subscriptions_revised_changelog_id,
  customers_history.stripe_customers_revised_changelog_id,
  subscriptions_history.subscription,
  customers_history.customer
FROM
  subscriptions_history
LEFT JOIN
  `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_customers_history_v1 AS customers_history
  ON subscriptions_history.subscription.customer_id = customers_history.customer.id
  AND subscriptions_history.valid_from >= customers_history.valid_from
  AND subscriptions_history.valid_from < customers_history.valid_to
UNION ALL
-- Include customer changes during the subscription history periods.
SELECT
  CONCAT(
    subscriptions_history.subscription.id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', customers_history.valid_from)
  ) AS id,
  customers_history.valid_from,
  LEAST(subscriptions_history.valid_to, customers_history.valid_to) AS valid_to,
  subscriptions_history.stripe_subscriptions_revised_changelog_id,
  customers_history.stripe_customers_revised_changelog_id,
  subscriptions_history.subscription,
  customers_history.customer
FROM
  subscriptions_history
JOIN
  `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_customers_history_v1 AS customers_history
  ON subscriptions_history.subscription.customer_id = customers_history.customer.id
  AND subscriptions_history.valid_from < customers_history.valid_from
  AND subscriptions_history.valid_to > customers_history.valid_from
