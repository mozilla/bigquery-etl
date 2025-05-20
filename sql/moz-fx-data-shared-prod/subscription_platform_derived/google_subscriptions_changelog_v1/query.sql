WITH purchases_changelog AS (
  SELECT
    document_id,
    `timestamp`,
    event_id,
    operation,
    mozfun.iap.parse_android_receipt(`data`) AS purchase
  FROM
    `moz-fx-fxa-prod-0712.firestore_export.iap_google_raw_changelog`
),
subscriptions_changelog AS (
  SELECT
    * EXCEPT (purchase),
    purchase AS subscription
  FROM
    purchases_changelog
  WHERE
    purchase.form_of_payment = 'GOOGLE_PLAY'
    AND purchase.sku_type = 'subs'
),
existing_subscriptions_changelog AS (
  {% if is_init() %}
    SELECT
      CAST(NULL AS STRING) AS purchase_token,
      CAST(NULL AS TIMESTAMP) AS max_timestamp
    FROM
      UNNEST([])
  {% else %}
    SELECT
      subscription.metadata.purchase_token,
      MAX(`timestamp`) AS max_timestamp
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_changelog_v1`
    GROUP BY
      subscription.metadata.purchase_token
  {% endif %}
),
subscriptions_new_changelog AS (
  SELECT
    subscriptions_changelog.*
  FROM
    subscriptions_changelog
  LEFT JOIN
    existing_subscriptions_changelog
    ON subscriptions_changelog.subscription.purchase_token = existing_subscriptions_changelog.purchase_token
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
    STRUCT(
      subscription.package_name,
      subscription.sku,
      subscription.sku_type,
      -- Deletion changelog records contain no subscription data, so we fall back to getting purchase token from the document ID.
      COALESCE(subscription.purchase_token, document_id) AS purchase_token,
      subscription.form_of_payment,
      subscription.is_mutable,
      subscription.latest_notification_type,
      subscription.replaced_by_another_purchase,
      subscription.user_id,
      subscription.verified_at
    ) AS metadata,
    subscription.kind,
    subscription.order_id,
    subscription.purchase_type,
    subscription.linked_purchase_token,
    subscription.payment_state,
    subscription.acknowledgement_state,
    subscription.start_time,
    subscription.expiry_time,
    subscription.auto_resume_time,
    subscription.auto_renewing,
    subscription.cancel_reason,
    subscription.user_cancellation_time,
    subscription.cancel_survey_result,
    subscription.price_currency_code,
    subscription.price_amount_micros,
    subscription.price_change,
    subscription.introductory_price_info,
    subscription.promotion_type,
    subscription.promotion_code,
    subscription.country_code,
    subscription.external_account_id,
    subscription.obfuscated_external_account_id,
    subscription.obfuscated_external_profile_id,
    subscription.developer_payload
  ) AS subscription
FROM
  subscriptions_new_changelog_deduped
