WITH unified AS (
    -- Combine newsletters and waitlists into a single set of records
  SELECT
    external_id,
    newsletter_name AS subscription_name,
    update_timestamp,
    IF(subscribed, 'subscribed', 'unsubscribed') AS subscription_state
  FROM
    `moz-fx-data-shared-prod.braze_derived.newsletters_v1`
  CROSS JOIN
    UNNEST(newsletters) AS newsletter
  UNION ALL
  SELECT
    external_id,
    CONCAT(waitlist_name, '-waitlist') AS subscription_name,
    update_timestamp,
    IF(subscribed, 'subscribed', 'unsubscribed') AS subscription_state
  FROM
    `moz-fx-data-shared-prod.braze_derived.waitlists_v1`
  CROSS JOIN
    UNNEST(waitlists) AS waitlist
),
all_subscriptions AS (
    -- Create a comprehensive list of all users and their potential subscriptions
  SELECT
    users.external_id,
    map.braze_subscription_name AS subscription_name,
    map.firefox_subscription_id,
    map.mozilla_subscription_id,
    map.mozilla_dev_subscription_id
  FROM
    `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  CROSS JOIN
    `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1` AS map
)
SELECT
  all_subscriptions.external_id,
  all_subscriptions.subscription_name,
  all_subscriptions.firefox_subscription_id,
  all_subscriptions.mozilla_subscription_id,
  all_subscriptions.mozilla_dev_subscription_id,
  COALESCE(unified.subscription_state, 'unsubscribed') AS subscription_state,
  unified.update_timestamp
FROM
  all_subscriptions
LEFT JOIN
  unified
  ON unified.external_id = all_subscriptions.external_id
  AND unified.subscription_name = all_subscriptions.subscription_name
ORDER BY
  all_subscriptions.external_id,
  all_subscriptions.subscription_name;
