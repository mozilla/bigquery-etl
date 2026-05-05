SELECT
  document_id AS subscription_id,
  `timestamp` AS firestore_export_timestamp,
  operation AS firestore_export_operation,
  JSON_VALUE(`data`, '$.status') AS subscription_status,
  JSON_VALUE(old_data, '$.status') AS previous_subscription_status
FROM
  `moz-fx-fxa-prod-0712.firestore_export.fxa_auth_prod_stripe_subscriptions_raw_changelog`
WHERE
  {% if is_init() %}
    DATE(`timestamp`) <= DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
  {% else %}
    DATE(`timestamp`) = @date
  {% endif %}
