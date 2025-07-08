WITH latest_existing_revised_changelog AS (
  {% if is_init() %}
    SELECT
      CAST(NULL AS TIMESTAMP) AS `timestamp`,
      CAST(NULL AS STRING) AS google_subscriptions_changelog_id,
      CAST(NULL AS STRING) AS firestore_export_event_id,
      CAST(NULL AS STRING) AS firestore_export_operation,
      IF(FALSE, subscription, NULL) AS subscription
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_changelog_v1`
    WHERE
      FALSE
  {% else %}
    SELECT
      `timestamp`,
      google_subscriptions_changelog_id,
      firestore_export_event_id,
      firestore_export_operation,
      subscription
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_revised_changelog_v1`
    QUALIFY
      1 = ROW_NUMBER() OVER (
        PARTITION BY
          subscription.metadata.purchase_token
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
    (
      latest_existing_revised_changelog.subscription.metadata.purchase_token IS NULL
      AND 1 = ROW_NUMBER() OVER subscription_changes_asc
    ) AS is_initial_subscription_changelog
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_changelog_v1` AS original_changelog
  LEFT JOIN
    latest_existing_revised_changelog
    ON original_changelog.subscription.metadata.purchase_token = latest_existing_revised_changelog.subscription.metadata.purchase_token
  WHERE
    original_changelog.firestore_export_operation != 'DELETE'
    AND (
      original_changelog.timestamp > latest_existing_revised_changelog.timestamp
      OR latest_existing_revised_changelog.timestamp IS NULL
    )
  WINDOW
    subscription_changes_asc AS (
      PARTITION BY
        original_changelog.subscription.metadata.purchase_token
      ORDER BY
        original_changelog.timestamp,
        original_changelog.id
    )
),
adjusted_new_original_changelog AS (
  SELECT
    original_id,
    (
      CASE
        -- Override the default 1970-01-01 00:00:00 timestamps for the records which were imported
        -- into BigQuery sometime between when the Firestore export table was created at
        -- 2021-10-18 19:07:20 and when the first change was recorded at 2021-10-18 20:52:31.
        WHEN firestore_export_operation = 'IMPORT'
          THEN STRUCT(
              TIMESTAMP('2021-10-18 20:00:00') AS `timestamp`,
              'adjusted_subscription_import' AS type
            )
        ELSE STRUCT(`timestamp`, type)
      END
    ).*,
    * EXCEPT (original_id, `timestamp`, type)
  FROM
    new_original_changelog
),
latest_new_original_changelog AS (
  SELECT
    *
  FROM
    adjusted_new_original_changelog
  QUALIFY
    1 = ROW_NUMBER() OVER (
      PARTITION BY
        subscription.metadata.purchase_token
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
    latest_existing_revised_changelog.google_subscriptions_changelog_id AS original_id,
    latest_existing_revised_changelog.firestore_export_event_id,
    latest_existing_revised_changelog.firestore_export_operation,
    latest_existing_revised_changelog.subscription
  FROM
    latest_existing_revised_changelog
  LEFT JOIN
    latest_new_original_changelog
    ON latest_existing_revised_changelog.subscription.metadata.purchase_token = latest_new_original_changelog.subscription.metadata.purchase_token
  WHERE
    latest_new_original_changelog.subscription.metadata.purchase_token IS NULL
),
synthetic_subscription_start_changelog AS (
  SELECT
    'synthetic_subscription_start' AS type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription.start_time AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          (
            SELECT AS STRUCT
              subscription.metadata.* REPLACE (
                TRUE AS is_mutable,
                CAST(NULL AS INTEGER) AS latest_notification_type,
                FALSE AS replaced_by_another_purchase,
                CAST(NULL AS TIMESTAMP) AS verified_at
              )
          ) AS metadata,
          IF(
            TIMESTAMP_DIFF(`timestamp`, subscription.start_time, DAY) < 28,
            subscription.expiry_time,
            `timestamp`
          ) AS expiry_time,
          CAST(NULL AS TIMESTAMP) AS auto_resume_time,
          TRUE AS auto_renewing,
          CAST(NULL AS INTEGER) AS cancel_reason,
          CAST(NULL AS TIMESTAMP) AS user_cancellation_time,
          CAST(
            NULL
            AS
              STRUCT<cancel_survey_reason INTEGER, user_input_cancel_reason STRING>
          ) AS cancel_survey_result
        )
    ) AS subscription
  FROM
    adjusted_new_original_changelog
  WHERE
    is_initial_subscription_changelog
    AND subscription.start_time < `timestamp`
),
synthetic_subscription_suspected_expiration_changelog AS (
  -- SubPlat hasn't consistently recorded Google subscription expirations, so we synthesize changelog
  -- records for expirations which seem to have occurred after the subscription's latest changelog.
  SELECT
    'synthetic_subscription_suspected_expiration' AS type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription.expiry_time AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          (SELECT AS STRUCT subscription.metadata.* REPLACE (FALSE AS is_mutable)) AS metadata,
          CAST(NULL AS INTEGER) AS payment_state
        )
    ) AS subscription
  FROM
    latest_changelog
  WHERE
    subscription.expiry_time > `timestamp`
    -- Wait at least 24 hours before assuming the subscription has expired, which will hopefully
    -- allow the majority of late-arriving changelog data to arrive (based on late-arriving data
    -- seen prior to 2025-06-04, waiting 24 hours would have covered ~95% of such cases).
    AND subscription.expiry_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
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
    adjusted_new_original_changelog
  UNION ALL
  SELECT
    `timestamp`,
    type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    synthetic_subscription_start_changelog
  UNION ALL
  SELECT
    `timestamp`,
    type,
    original_id,
    firestore_export_event_id,
    firestore_export_operation,
    subscription
  FROM
    synthetic_subscription_suspected_expiration_changelog
)
SELECT
  CONCAT(
    subscription.metadata.purchase_token,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`),
    '-',
    firestore_export_operation,
    COALESCE(CONCAT('-', NULLIF(firestore_export_event_id, '')), '')
  ) AS id,
  `timestamp`,
  CURRENT_TIMESTAMP() AS created_at,
  type,
  original_id AS google_subscriptions_changelog_id,
  firestore_export_event_id,
  firestore_export_operation,
  subscription
FROM
  changelog_union
