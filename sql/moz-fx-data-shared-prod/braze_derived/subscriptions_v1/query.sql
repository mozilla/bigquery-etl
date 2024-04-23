WITH unified AS (
  -- Combine newsletters and waitlists into a single set of records from user_profiles
  SELECT
    external_id,
    newsletter.newsletter_name AS subscription_name,  -- No change to name for newsletters
    newsletter.update_timestamp,
    IF(newsletter.subscribed, 'subscribed', 'unsubscribed') AS subscription_state
  FROM
    `moz-fx-data-shared-prod.braze_derived.user_profiles_v1`,
    UNNEST(newsletters) AS newsletter
  UNION ALL
  SELECT
    external_id,
    CONCAT(
      waitlist.waitlist_name,
      '-waitlist'
    ) AS subscription_name,  -- Add '-waitlist' suffix for clarity
    waitlist.update_timestamp,
    IF(waitlist.subscribed, 'subscribed', 'unsubscribed') AS subscription_state
  FROM
    `moz-fx-data-shared-prod.braze_derived.user_profiles_v1`,
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
),
subscriptions_mapped AS (
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
)
SELECT
  subscriptions_mapped.external_id AS external_id,
  ARRAY_AGG(
    STRUCT(
      subscriptions_mapped.subscription_name AS subscription_name,
      subscriptions_mapped.firefox_subscription_id AS firefox_subscription_id,
      subscriptions_mapped.mozilla_subscription_id AS mozilla_subscription_id,
      subscriptions_mapped.mozilla_dev_subscription_id AS mozilla_dev_subscription_id,
      subscriptions_mapped.subscription_state AS subscription_state,
      subscriptions_mapped.update_timestamp AS update_timestamp
    )
    ORDER BY
      subscriptions_mapped.subscription_name ASC
  ) AS subscriptions
FROM
  subscriptions_mapped
GROUP BY
  subscriptions_mapped.external_id;
