-- Query for braze_external.monitor_plus_sync_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  CASE
    WHEN source_file = 'ppp_auto_renewing'
      THEN CONCAT('ppp-auto-renewing-', SUBSTR(TO_HEX(SHA256(GENERATE_UUID())), 1, 23))
    WHEN source_file = 'ppp_non_renewing'
      THEN CONCAT('ppp-non-renewing-', SUBSTR(TO_HEX(SHA256(GENERATE_UUID())), 1, 23))
    WHEN source_file = 'all_monitor_plus'
      THEN CONCAT('all-monitor-plus-', SUBSTR(TO_HEX(SHA256(GENERATE_UUID())), 1, 23))
  END AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      email AS email,
      'subscribed' AS email_subscribe,
      ARRAY_AGG(
        STRUCT(
          CASE
            WHEN source_file = 'ppp_auto_renewing'
              THEN '25b6f3c1-aec4-43c2-962d-ff08fd51de37'
            WHEN source_file = 'ppp_non_renewing'
              THEN '1e65097d-fac6-424a-8c1b-745b28559aea'
            WHEN source_file = 'all_monitor_plus'
              THEN '25b6f3c1-aec4-43c2-962d-ff08fd51de37'
            ELSE 'unknown'
          END AS subscription_group_id,
          'subscribed' AS subscription_state
        )
      ) AS subscription_groups
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.monitor_plus_users_v1`
GROUP BY
  email,
  source_file
UNION ALL
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  CONCAT('monitor-free-users-', SUBSTR(TO_HEX(SHA256(GENERATE_UUID())), 1, 23)) AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      email AS email,
      'subscribed' AS email_subscribe,
      ARRAY_AGG(
        STRUCT(
          '87c4fd94-90a3-443c-bfb0-97d1227219aa' AS subscription_group_id,
          'subscribed' AS subscription_state
        )
      ) AS subscription_groups
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_external.monitor_plus_sunset_v1`
GROUP BY
  email;
