WITH purchases_changelog AS (
  SELECT
    document_id,
    `timestamp`,
    event_id,
    operation,
    mozfun.iap.parse_apple_event(`data`) AS purchase
  FROM
    `moz-fx-fxa-prod-0712.firestore_export.iap_app_store_purchases_raw_changelog`
),
subscriptions_changelog AS (
  SELECT
    * EXCEPT (purchase),
    purchase AS subscription
  FROM
    purchases_changelog
  -- This filter is mimicking some of SubPlat's internal logic:
  -- https://github.com/mozilla/fxa/blob/e73499ee02cfc67a81e3f4cb21742fb95e00d1ea/packages/fxa-shared/payments/iap/apple-app-store/purchase-manager.ts#L194-L196
  -- Using LOGICAL_OR() to also include associated deletion changelog records, where the data values would be null.
  QUALIFY
    LOGICAL_OR(
      purchase.form_of_payment = 'APPLE_APP_STORE'
      AND purchase.type = 'Auto-Renewable Subscription'
    ) OVER (PARTITION BY document_id)
),
existing_subscriptions_changelog AS (
  {% if is_init() %}
    SELECT
      CAST(NULL AS STRING) AS original_transaction_id,
      CAST(NULL AS TIMESTAMP) AS max_timestamp
    FROM
      UNNEST([])
  {% else %}
    SELECT
      subscription.original_transaction_id,
      MAX(`timestamp`) AS max_timestamp
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_changelog_v1`
    GROUP BY
      subscription.original_transaction_id
  {% endif %}
),
subscriptions_new_changelog AS (
  SELECT
    subscriptions_changelog.*
  FROM
    subscriptions_changelog
  LEFT JOIN
    existing_subscriptions_changelog
    ON subscriptions_changelog.document_id = existing_subscriptions_changelog.original_transaction_id
  WHERE
    subscriptions_changelog.timestamp > existing_subscriptions_changelog.max_timestamp
    OR existing_subscriptions_changelog.max_timestamp IS NULL
),
subscriptions_new_changelog_deduped AS (
  -- Sometimes we get duplicate records in BigQuery for the same Firestore document change.
  SELECT
    document_id,
    MIN(`timestamp`) AS `timestamp`,
    event_id,
    operation,
    MIN_BY(subscription, `timestamp`) AS subscription
  FROM
    subscriptions_new_changelog
  GROUP BY
    document_id,
    event_id,
    operation
)
SELECT
  CONCAT(
    document_id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`),
    '-',
    operation,
    -- The initial import records don't have event IDs.
    COALESCE(CONCAT('-', NULLIF(event_id, '')), '')
  ) AS id,
  `timestamp`,
  event_id AS firestore_export_event_id,
  operation AS firestore_export_operation,
  STRUCT(
    -- https://developer.apple.com/documentation/appstoreserverapi/statusresponse
    subscription.environment,
    subscription.bundle_id,
    -- https://developer.apple.com/documentation/appstoreserverapi/lasttransactionsitem
    -- Deletion changelog records contain no subscription data, so we fall back to getting original transaction ID from the document ID.
    COALESCE(subscription.original_transaction_id, document_id) AS original_transaction_id,
    subscription.status,
    -- https://developer.apple.com/documentation/appstoreserverapi/jwstransactiondecodedpayload
    STRUCT(
      subscription.currency,
      subscription.expires_date,
      subscription.in_app_ownership_type,
      subscription.is_upgraded,
      subscription.offer_identifier,
      subscription.offer_type,
      subscription.original_purchase_date,
      subscription.price,
      subscription.product_id,
      subscription.purchase_date,
      subscription.revocation_date,
      subscription.revocation_reason,
      subscription.storefront,
      subscription.transaction_id,
      subscription.type
    ) AS last_transaction,
    -- https://developer.apple.com/documentation/appstoreserverapi/jwsrenewalinfodecodedpayload
    STRUCT(
      subscription.auto_renew_product_id,
      subscription.auto_renew_status,
      subscription.renewal_currency AS currency,
      subscription.expiration_intent,
      subscription.grace_period_expires_date,
      subscription.is_in_billing_retry AS is_in_billing_retry_period,
      subscription.renewal_offer_identifier AS offer_identifier,
      subscription.renewal_offer_type AS offer_type,
      subscription.renewal_price
    ) AS renewal_info,
    STRUCT(
      subscription.form_of_payment,
      subscription.latest_notification_type,
      subscription.latest_notification_subtype,
      subscription.user_id,
      subscription.verified_at
    ) AS metadata
  ) AS subscription
FROM
  subscriptions_new_changelog_deduped
