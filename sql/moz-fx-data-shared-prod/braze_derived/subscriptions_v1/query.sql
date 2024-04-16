SELECT
  unified.external_id AS external_id,
  unified.subscription_name AS subscription_name,
  map.firefox_subscription_id AS firefox_subscription_id,
  map.mozilla_subscription_id AS mozilla_subscription_id,
  unified.subscription_state AS subscription_state,
  CURRENT_TIMESTAMP() AS last_modified_timestamp,
FROM
  (
  -- Combine newsletters and waitlists into a single set of records
    SELECT
      external_id,
      newsletter_name AS subscription_name,
      IF(subscribed, 'subscribed', 'unsubscribed') AS subscription_state
    FROM
      `moz-fx-data-shared-prod.braze_derived.newsletters_v1`,
      UNNEST(newsletters) AS newsletter
    UNION ALL
    SELECT
      external_id,
      CONCAT(waitlist_name, '-waitlist') AS subscription_name, -- Appending '-waitlist' to the name
      IF(subscribed, 'subscribed', 'unsubscribed') AS subscription_state
    FROM
      `moz-fx-data-shared-prod.braze_derived.waitlists_v1`,
      UNNEST(waitlists) AS waitlist
  ) unified
JOIN
  `moz-fx-data-shared-prod.braze_derived.subscriptions_map_v1` map
  ON unified.subscription_name = map.braze_subscription_name;
