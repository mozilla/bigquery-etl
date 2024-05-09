-- Construct the JSON payload in Braze required format
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
GROUP BY
  subscriptions.external_id;
