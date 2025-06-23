WITH latest_existing_revised_changelog AS (
  {% if is_init() %}
    SELECT
      CAST(NULL AS TIMESTAMP) AS `timestamp`,
      CAST(NULL AS STRING) AS apple_subscriptions_changelog_id,
      CAST(NULL AS STRING) AS firestore_export_event_id,
      CAST(NULL AS STRING) AS firestore_export_operation,
      IF(FALSE, subscription, NULL) AS subscription
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_changelog_v1`
    WHERE
      FALSE
  {% else %}
    SELECT
      `timestamp`,
      apple_subscriptions_changelog_id,
      firestore_export_event_id,
      firestore_export_operation,
      subscription
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_revised_changelog_v1`
    QUALIFY
      1 = ROW_NUMBER() OVER (
        PARTITION BY
          subscription.original_transaction_id
        ORDER BY
          `timestamp` DESC,
          id DESC
      )
  {% endif %}
),
new_original_changelog AS (
  SELECT
    original_changelog.id AS original_id,
    original_changelog.timestamp,
    'original' AS type,
    original_changelog.firestore_export_event_id,
    original_changelog.firestore_export_operation,
    original_changelog.subscription,
    COALESCE(
      LAG(original_changelog.timestamp) OVER subscription_changes_asc,
      latest_existing_revised_changelog.timestamp
    ) AS previous_subscription_change_at,
    COALESCE(
      LAG(
        original_changelog.subscription.last_transaction.purchase_date
      ) OVER subscription_changes_asc,
      latest_existing_revised_changelog.subscription.last_transaction.purchase_date
    ) AS previous_subscription_purchase_date
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_changelog_v1` AS original_changelog
  LEFT JOIN
    latest_existing_revised_changelog
    ON original_changelog.subscription.original_transaction_id = latest_existing_revised_changelog.subscription.original_transaction_id
  WHERE
    original_changelog.firestore_export_operation != 'DELETE'
    AND (
      original_changelog.timestamp > latest_existing_revised_changelog.timestamp
      OR latest_existing_revised_changelog.timestamp IS NULL
    )
  WINDOW
    subscription_changes_asc AS (
      PARTITION BY
        original_changelog.subscription.original_transaction_id
      ORDER BY
        original_changelog.timestamp,
        original_changelog.id
    )
),
latest_new_original_changelog AS (
  SELECT
    *
  FROM
    new_original_changelog
  QUALIFY
    1 = ROW_NUMBER() OVER (
      PARTITION BY
        subscription.original_transaction_id
      ORDER BY
        `timestamp` DESC,
        original_id DESC
    )
),
latest_changelog AS (
  SELECT
    `timestamp`,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    latest_new_original_changelog
  UNION ALL
  SELECT
    latest_existing_revised_changelog.timestamp,
    latest_existing_revised_changelog.apple_subscriptions_changelog_id AS original_id,
    latest_existing_revised_changelog.firestore_export_event_id,
    latest_existing_revised_changelog.firestore_export_operation,
    latest_existing_revised_changelog.subscription
  FROM
    latest_existing_revised_changelog
  LEFT JOIN
    latest_new_original_changelog
    ON latest_existing_revised_changelog.subscription.original_transaction_id = latest_new_original_changelog.subscription.original_transaction_id
  WHERE
    latest_new_original_changelog.subscription.original_transaction_id IS NULL
),
synthetic_subscription_period_start_changelog AS (
  -- Synthesize changelog records representing the exact start of each new subscription period when possible.
  SELECT
    'synthetic_subscription_period_start' AS type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription.last_transaction.purchase_date AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          1 AS status,  -- 1 = active
          (
            SELECT AS STRUCT
              subscription.renewal_info.* REPLACE (
                CAST(NULL AS INTEGER) AS expiration_intent,
                CAST(NULL AS TIMESTAMP) AS grace_period_expires_date
              )
          ) AS renewal_info,
          (
            SELECT AS STRUCT
              subscription.metadata.* REPLACE (
                CAST(NULL AS STRING) AS latest_notification_type,
                CAST(NULL AS STRING) AS latest_notification_subtype,
                CAST(NULL AS TIMESTAMP) AS verified_at
              )
          ) AS metadata
        )
    ) AS subscription
  FROM
    new_original_changelog
  WHERE
    subscription.last_transaction.purchase_date IS DISTINCT FROM previous_subscription_purchase_date
    AND subscription.last_transaction.purchase_date < `timestamp`
    AND (
      subscription.last_transaction.purchase_date > previous_subscription_change_at
      OR previous_subscription_change_at IS NULL
    )
    AND subscription.status IN (
      1,  -- 1 = active
      4   -- 4 = in billing grace period
    )
),
synthetic_suspected_subscription_expiration_changelog AS (
  -- SubPlat hasn't consistently recorded all Apple subscription changes, so we synthesize changelog records
  -- for expirations which seem to have occurred after the subscription's latest changelog.
  SELECT
    'synthetic_suspected_subscription_expiration' AS type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    GREATEST(
      subscription.last_transaction.expires_date,
      TIMESTAMP_ADD(`timestamp`, INTERVAL 1 MICROSECOND)
    ) AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          2 AS status,  -- 2 = expired
          (
            SELECT AS STRUCT
              subscription.renewal_info.* REPLACE (CAST(NULL AS INTEGER) AS expiration_intent)
          ) AS renewal_info
        )
    ) AS subscription
  FROM
    latest_changelog
  WHERE
    subscription.status = 1  -- 1 = active
    AND subscription.renewal_info.auto_renew_status = 0  -- 0 = auto-renewal off
    -- Wait at least 24 hours before assuming the subscription has expired, which will
    -- hopefully allow the majority of late-arriving changelog data to arrive.
    AND GREATEST(subscription.last_transaction.expires_date, `timestamp`) < TIMESTAMP_SUB(
      CURRENT_TIMESTAMP(),
      INTERVAL 24 HOUR
    )
),
synthetic_suspected_billing_retry_period_start_changelog AS (
  -- SubPlat hasn't consistently recorded all Apple subscription changes, so we synthesize changelog records
  -- for billing retry periods which seem to have occurred after the subscription's latest changelog.
  SELECT
    'synthetic_suspected_billing_retry_period_start' AS type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    GREATEST(
      IF(
        subscription.status = 4,  -- 4 = in billing grace period
        COALESCE(
          subscription.renewal_info.grace_period_expires_date,
          subscription.last_transaction.expires_date
        ),
        subscription.last_transaction.expires_date
      ),
      TIMESTAMP_ADD(`timestamp`, INTERVAL 1 MICROSECOND)
    ) AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          3 AS status,  -- 3 = in billing retry period
          (
            SELECT AS STRUCT
              subscription.renewal_info.* REPLACE (TRUE AS is_in_billing_retry_period)
          ) AS renewal_info
        )
    ) AS subscription
  FROM
    latest_changelog
  WHERE
    (
      (
        subscription.status = 1  -- 1 = active
        AND subscription.renewal_info.auto_renew_status = 1  -- 1 = auto-renewal on
      )
      OR subscription.status = 4  -- 4 = in billing grace period
    )
    -- Wait at least 24 hours before assuming the subscription has entered billing retry,
    -- which will hopefully allow the majority of late-arriving changelog data to arrive.
    AND GREATEST(
      IF(
        subscription.status = 4,  -- 4 = in billing grace period
        COALESCE(
          subscription.renewal_info.grace_period_expires_date,
          subscription.last_transaction.expires_date
        ),
        subscription.last_transaction.expires_date
      ),
      `timestamp`
    ) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
),
synthetic_suspected_billing_retry_period_expiration_changelog AS (
  -- SubPlat hasn't consistently recorded all Apple subscription changes, so we synthesize changelog records
  -- for billing retry period expirations which seem to have occurred after the subscription's latest changelog.
  SELECT
    'synthetic_suspected_billing_retry_period_expiration' AS type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    GREATEST(
      TIMESTAMP_ADD(subscription.last_transaction.expires_date, INTERVAL 60 DAY),
      TIMESTAMP_ADD(`timestamp`, INTERVAL 1 MICROSECOND)
    ) AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          2 AS status,  -- 2 = expired
          (
            SELECT AS STRUCT
              subscription.renewal_info.* REPLACE (CAST(NULL AS INTEGER) AS expiration_intent)
          ) AS renewal_info
        )
    ) AS subscription
  FROM
    latest_changelog
  WHERE
    subscription.status = 3  -- 3 = in billing retry period
    -- Wait at least 24 hours after the 60-day billing retry period ends before assuming the subscription
    -- has expired, which will hopefully allow the majority of late-arriving changelog data to arrive.
    AND GREATEST(
      TIMESTAMP_ADD(subscription.last_transaction.expires_date, INTERVAL 60 DAY),
      `timestamp`
    ) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
),
changelog_union AS (
  SELECT
    `timestamp`,
    type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    new_original_changelog
  UNION ALL
  SELECT
    `timestamp`,
    type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    synthetic_subscription_period_start_changelog
  UNION ALL
  SELECT
    `timestamp`,
    type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    synthetic_suspected_subscription_expiration_changelog
  UNION ALL
  SELECT
    `timestamp`,
    type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    synthetic_suspected_billing_retry_period_start_changelog
  UNION ALL
  SELECT
    `timestamp`,
    type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    synthetic_suspected_billing_retry_period_expiration_changelog
)
SELECT
  CONCAT(
    subscription.original_transaction_id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`),
    '-',
    firestore_export_operation,
    COALESCE(CONCAT('-', NULLIF(firestore_export_event_id, '')), '')
  ) AS id,
  `timestamp`,
  CURRENT_TIMESTAMP() AS created_at,
  type,
  original_id AS apple_subscriptions_changelog_id,
  firestore_export_event_id,
  firestore_export_operation,
  subscription
FROM
  changelog_union
