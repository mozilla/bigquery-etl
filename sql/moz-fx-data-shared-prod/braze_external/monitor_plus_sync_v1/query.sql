-- Query for braze_external.monitor_plus_sync_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  CONCAT('monitor-plus_users-', SUBSTR(TO_HEX(SHA256(GENERATE_UUID())), 1, 23)) AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      email AS email,
      "subscribed" AS email_subscribe,
      ARRAY_AGG(
        STRUCT("***" AS subscription_group_id, "subscribed" AS subscription_state)
      ) AS subscription_groups
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_external.monitor_plus_sunset_test`
GROUP BY
  email
