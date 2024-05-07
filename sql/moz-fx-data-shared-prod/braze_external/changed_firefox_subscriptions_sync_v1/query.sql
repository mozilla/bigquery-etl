-- CTE to determine the maximum update timestamp from changed_subscriptions_v1
WITH max_update AS (
  SELECT
    MAX(subscriptions.update_timestamp) AS latest_subscription_updated_at
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_subscriptions_v1` AS changed,
    UNNEST(changed.subscriptions) AS subscriptions
),
-- Construct the JSON payload in Braze required format
subscriptions_updated AS (
  SELECT
    CURRENT_TIMESTAMP() AS UPDATED_AT,
    subscriptions.external_id AS EXTERNAL_ID,
    TO_JSON(
      STRUCT(
        ARRAY_AGG(
          STRUCT(
            subscriptions_array.firefox_subscription_id AS subscription_group_id,
            subscriptions_array.subscription_state AS subscription_state
          )
          ORDER BY
            subscriptions_array.update_timestamp DESC
        ) AS subscription_groups
      )
    ) AS PAYLOAD
  FROM
    `moz-fx-data-shared-prod.braze_external.changed_subscriptions_v1` AS subscriptions
  CROSS JOIN
    UNNEST(subscriptions.subscriptions) AS subscriptions_array
  JOIN
    max_update
    ON subscriptions_array.update_timestamp > max_update.latest_subscription_updated_at
  GROUP BY
    subscriptions.external_id
)
SELECT
  *
FROM
  subscriptions_updated;
