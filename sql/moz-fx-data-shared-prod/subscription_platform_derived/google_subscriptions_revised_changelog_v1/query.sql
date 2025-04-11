WITH existing_revised_changelog AS (
  {% if is_init() %}
    SELECT
      CAST(NULL AS STRING) AS purchase_token,
      CAST(NULL AS INTEGER) AS subscription_change_count,
      CAST(NULL AS TIMESTAMP) AS max_timestamp
    FROM
      UNNEST([])
  {% else %}
    SELECT
      subscription.metadata.purchase_token,
      COUNT(*) AS subscription_change_count,
      MAX(`timestamp`) AS max_timestamp
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_revised_changelog_v1`
    GROUP BY
      subscription.metadata.purchase_token
  {% endif %}
),
original_changelog AS (
  SELECT
    original_changelog.*,
    (
      COALESCE(existing_revised_changelog.subscription_change_count, 0) + (
        ROW_NUMBER() OVER (
          PARTITION BY
            original_changelog.subscription.metadata.purchase_token
          ORDER BY
            original_changelog.timestamp,
            original_changelog.id
        )
      )
    ) AS subscription_change_number
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_changelog_v1` AS original_changelog
  LEFT JOIN
    existing_revised_changelog
    ON original_changelog.subscription.metadata.purchase_token = existing_revised_changelog.purchase_token
  WHERE
    original_changelog.firestore_export_operation != 'DELETE'
    AND (
      original_changelog.timestamp > existing_revised_changelog.max_timestamp
      OR existing_revised_changelog.max_timestamp IS NULL
    )
),
adjusted_original_changelog AS (
  SELECT
    id AS original_id,
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
        ELSE STRUCT(`timestamp`, 'original' AS type)
      END
    ).*,
    * EXCEPT (id, `timestamp`)
  FROM
    original_changelog
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
    adjusted_original_changelog
  WHERE
    subscription_change_number = 1
    AND subscription.start_time < `timestamp`
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
    adjusted_original_changelog
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
  subscription
FROM
  changelog_union
