-- Query for braze_external.delete_monitor_users_fix_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  UPDATED_AT,
  EXTERNAL_ID
FROM
  `moz-fx-data-shared-prod.braze_external.monitor_plus_sync_v1`
