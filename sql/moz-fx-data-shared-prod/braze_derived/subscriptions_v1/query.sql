WITH unified AS (
  -- Combine newsletters and waitlists into a single set of records
  SELECT
    external_id,
    newsletter_name AS subscription_name,
    IF(subscribed, 'subscribed', 'unsubscribed') AS subscription_state
  FROM
    `moz-fx-data-shared-prod.braze_derived.newsletters_v1`
  CROSS JOIN
    UNNEST(newsletters) AS newsletter
  UNION ALL
  SELECT
    external_id,
    CONCAT(waitlist_name, '-waitlist') AS subscription_name, -- Appending '-waitlist' to the name
    IF(subscribed, 'subscribed', 'unsubscribed') AS subscription_state
  FROM
    `moz-fx-data-shared-prod.braze_derived.waitlists_v1`
  CROSS JOIN
    UNNEST(waitlists) AS waitlist
)

SELECT
  unified.external_id AS external_id,
  unified.subscription_name AS subscription_name,
  map.firefox_subscription_id AS firefox_subscription_id,
  map.mozilla_subscription_id AS mozilla_subscription_id,
  unified.subscription_state AS subscription_state
FROM
  unified
INNER JOIN
  `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1` AS map
  ON unified.subscription_name = map.braze_subscription_name
-- Ensure users are active/not suppressed
INNER JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  ON users.external_id = unified.external_id;
