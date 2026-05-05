-- Construct the JSON payload in Braze required format
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  external_id AS EXTERNAL_ID
FROM
  `moz-fx-data-shared-prod.braze_external.changed_users_v1`
WHERE
  status = 'Deleted'
