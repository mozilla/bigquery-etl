WITH unified AS (
  SELECT
    external_id,
    subscription_name,
    MAX(update_timestamp) AS update_timestamp,
    MAX(subscription_state) AS subscription_state
  FROM (
    SELECT
      external_id,
      newsletter.newsletter_name AS subscription_name,
      newsletter.update_timestamp,
      IF(newsletter.subscribed, 'subscribed', 'unsubscribed') AS subscription_state
    FROM
      `moz-fx-data-shared-prod.braze_derived.user_profiles_v1`,
      UNNEST(newsletters) AS newsletter
    UNION ALL
    SELECT
      external_id,
      CASE 
        WHEN waitlist.waitlist_name = 'vpn' THEN 'guardian-vpn-waitlist'
        ELSE CONCAT(waitlist.waitlist_name, '-waitlist')
      END AS subscription_name,
      waitlist.update_timestamp,
      IF(waitlist.subscribed, 'subscribed', 'unsubscribed') AS subscription_state
    FROM
      `moz-fx-data-shared-prod.braze_derived.user_profiles_v1`,
      UNNEST(waitlists) AS waitlist
  ) AS unified_data
  GROUP BY
    external_id, subscription_name
),
subscriptions_mapped AS (
  SELECT
    unified.external_id,
    unified.subscription_name,
    map.firefox_subscription_id,
    map.mozilla_subscription_id,
    map.mozilla_dev_subscription_id,
    unified.subscription_state,
    unified.update_timestamp
  FROM
    unified
  JOIN
    `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1` AS map
  ON
    unified.subscription_name = map.braze_subscription_name
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
  subscriptions_mapped.external_id
HAVING
  COUNT(subscriptions_mapped.subscription_name) > 0; -- Only include rows where subscription IDs are not null
