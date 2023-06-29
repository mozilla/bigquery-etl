WITH original_changelog AS (
  SELECT
    id,
    `timestamp`,
    synced_at,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          STRUCT(
            JSON_VALUE(subscription.metadata.appliedPromotionCode) AS appliedPromotionCode,
            TIMESTAMP_SECONDS(
              CAST(JSON_VALUE(subscription.metadata.cancelled_for_customer_at) AS INT64)
            ) AS cancelled_for_customer_at,
            TIMESTAMP_SECONDS(
              CAST(JSON_VALUE(subscription.metadata.plan_change_date) AS INT64)
            ) AS plan_change_date,
            JSON_VALUE(subscription.metadata.previous_plan_id) AS previous_plan_id
          ) AS metadata
        )
    ) AS subscription,
    ROW_NUMBER() OVER subscription_changes AS subscription_change_number,
    LEAD(`timestamp`) OVER subscription_changes AS next_subscription_change_at,
    LAG(subscription.ended_at) OVER subscription_changes AS previous_subscription_ended_at
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_subscriptions_changelog_v1
  WINDOW
    subscription_changes AS (
      PARTITION BY
        subscription.id
      ORDER BY
        `timestamp`
    )
),
adjusted_original_changelog AS (
  SELECT
    id AS original_id,
    (
      CASE
        -- Sometimes Fivetran's initial timestamp for a subscription is after it was created.
        WHEN subscription_change_number = 1
          AND subscription.created < `timestamp`
          THEN STRUCT(subscription.created AS `timestamp`, 'adjusted_subscription_start' AS type)
        -- Sometimes Fivetran's timestamp for a subscription ending is after it was ended.
        WHEN subscription.ended_at IS NOT NULL
          AND previous_subscription_ended_at IS NULL
          AND subscription_change_number > 1
          AND subscription.ended_at < `timestamp`
          THEN STRUCT(subscription.ended_at AS `timestamp`, 'adjusted_subscription_end' AS type)
        ELSE STRUCT(`timestamp` AS `timestamp`, 'original' AS type)
      END
    ).*,
    * EXCEPT (id, `timestamp`)
  FROM
    original_changelog
),
-- Between 2023-02-24 and 2023-02-26 Fivetran did a full resync of the `subscription_history` table,
-- overwriting/deleting existing history records for most subscriptions and replacing them with a
-- single history record per subscription purporting to be in effect from the start of the subscription,
-- but actually reflecting the current state of the subscription at the time it was resynced (DENG-754).
-- As a result, we have to adjust and synthesize records to more accurately reconstruct history.
questionable_resync_changelog AS (
  SELECT
    original_id,
    `timestamp`,
    synced_at,
    subscription,
    subscription.items[0].plan.id AS subscription_plan_id,
    next_subscription_change_at
  FROM
    adjusted_original_changelog
  WHERE
    (DATE(synced_at) BETWEEN '2023-02-24' AND '2023-02-26')
    AND subscription_change_number = 1
),
synthetic_subscription_start_changelog AS (
  SELECT
    'synthetic_subscription_start' AS type,
    original_id,
    `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          CAST(NULL AS TIMESTAMP) AS cancel_at,
          FALSE AS cancel_at_period_end,
          CAST(NULL AS TIMESTAMP) AS canceled_at,
          IF(
            subscription.current_period_start = subscription.start_date,
            subscription.current_period_end,
            NULL
          ) AS current_period_end,
          IF(
            subscription.current_period_start = subscription.start_date,
            subscription.current_period_start,
            NULL
          ) AS current_period_start,
          IF(
            subscription.discount.start <= subscription.start_date,
            subscription.discount,
            NULL
          ) AS discount,
          CAST(NULL AS TIMESTAMP) AS ended_at,
          IF(
            subscription.current_period_start = subscription.start_date,
            subscription.latest_invoice_id,
            NULL
          ) AS latest_invoice_id,
          STRUCT(
            subscription.metadata.appliedPromotionCode,
            CAST(NULL AS TIMESTAMP) AS cancelled_for_customer_at,
            CAST(NULL AS TIMESTAMP) AS plan_change_date,
            CAST(NULL AS STRING) AS previous_plan_id
          ) AS metadata,
          CASE
            WHEN subscription.status IN ('incomplete', 'incomplete_expired')
              THEN 'incomplete'
            WHEN subscription.status = 'canceled'
              AND TIMESTAMP_DIFF(subscription.ended_at, subscription.start_date, HOUR) < 24
              THEN 'incomplete'
            WHEN subscription.trial_start = subscription.start_date
              THEN 'trialing'
            ELSE 'active'
          END AS status,
          IF(
            subscription.trial_start = subscription.start_date,
            subscription.trial_end,
            NULL
          ) AS trial_end,
          IF(
            subscription.trial_start = subscription.start_date,
            subscription.trial_start,
            NULL
          ) AS trial_start
        )
    ) AS subscription,
    IF(
      subscription.metadata.previous_plan_id IS NOT NULL
      AND subscription.metadata.plan_change_date IS NOT NULL,
      subscription.metadata.previous_plan_id,
      subscription_plan_id
    ) AS subscription_plan_id
  FROM
    questionable_resync_changelog
),
synthetic_plan_change_changelog AS (
  SELECT
    'synthetic_plan_change' AS type,
    original_id,
    subscription.metadata.plan_change_date AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          IF(
            subscription.cancel_at_period_end
            AND subscription.canceled_at <= subscription.metadata.plan_change_date,
            subscription.cancel_at,
            NULL
          ) AS cancel_at,
          IF(
            subscription.cancel_at_period_end
            AND subscription.canceled_at <= subscription.metadata.plan_change_date,
            TRUE,
            FALSE
          ) AS cancel_at_period_end,
          IF(
            subscription.canceled_at <= subscription.metadata.plan_change_date,
            subscription.canceled_at,
            NULL
          ) AS canceled_at,
          IF(
            subscription.current_period_start <= subscription.metadata.plan_change_date,
            subscription.current_period_end,
            NULL
          ) AS current_period_end,
          IF(
            subscription.current_period_start <= subscription.metadata.plan_change_date,
            subscription.current_period_start,
            NULL
          ) AS current_period_start,
          IF(
            subscription.discount.start <= subscription.metadata.plan_change_date,
            subscription.discount,
            NULL
          ) AS discount,
          CAST(NULL AS TIMESTAMP) AS ended_at,
          IF(
            subscription.current_period_start <= subscription.metadata.plan_change_date,
            subscription.latest_invoice_id,
            NULL
          ) AS latest_invoice_id,
          STRUCT(
            subscription.metadata.appliedPromotionCode,
            IF(
              subscription.metadata.cancelled_for_customer_at <= subscription.metadata.plan_change_date,
              subscription.metadata.cancelled_for_customer_at,
              NULL
            ) AS cancelled_for_customer_at,
            subscription.metadata.plan_change_date,
            subscription.metadata.previous_plan_id
          ) AS metadata,
          IF(
            subscription.metadata.plan_change_date >= subscription.trial_start
            AND subscription.metadata.plan_change_date < subscription.trial_end,
            'trialing',
            'active'
          ) AS status,
          IF(
            subscription.trial_start <= subscription.metadata.plan_change_date,
            subscription.trial_end,
            NULL
          ) AS trial_end,
          IF(
            subscription.trial_start <= subscription.metadata.plan_change_date,
            subscription.trial_start,
            NULL
          ) AS trial_start
        )
    ) AS subscription,
    subscription_plan_id
  FROM
    questionable_resync_changelog
  WHERE
    subscription.metadata.previous_plan_id IS NOT NULL
    AND subscription.metadata.plan_change_date IS NOT NULL
),
synthetic_trial_start_changelog AS (
  SELECT
    'synthetic_trial_start' AS type,
    original_id,
    subscription.trial_start AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          IF(
            subscription.cancel_at_period_end
            AND subscription.canceled_at <= subscription.trial_start,
            subscription.cancel_at,
            NULL
          ) AS cancel_at,
          IF(
            subscription.cancel_at_period_end
            AND subscription.canceled_at <= subscription.trial_start,
            TRUE,
            FALSE
          ) AS cancel_at_period_end,
          IF(
            subscription.canceled_at <= subscription.trial_start,
            subscription.canceled_at,
            NULL
          ) AS canceled_at,
          IF(
            subscription.current_period_start <= subscription.trial_start,
            subscription.current_period_end,
            NULL
          ) AS current_period_end,
          IF(
            subscription.current_period_start <= subscription.trial_start,
            subscription.current_period_start,
            NULL
          ) AS current_period_start,
          IF(
            subscription.discount.start <= subscription.trial_start,
            subscription.discount,
            NULL
          ) AS discount,
          CAST(NULL AS TIMESTAMP) AS ended_at,
          IF(
            subscription.current_period_start <= subscription.trial_start,
            subscription.latest_invoice_id,
            NULL
          ) AS latest_invoice_id,
          STRUCT(
            subscription.metadata.appliedPromotionCode,
            IF(
              subscription.metadata.cancelled_for_customer_at <= subscription.trial_start,
              subscription.metadata.cancelled_for_customer_at,
              NULL
            ) AS cancelled_for_customer_at,
            IF(
              subscription.metadata.plan_change_date <= subscription.trial_start,
              subscription.metadata.plan_change_date,
              NULL
            ) AS plan_change_date,
            IF(
              subscription.metadata.plan_change_date <= subscription.trial_start,
              subscription.metadata.previous_plan_id,
              NULL
            ) AS previous_plan_id
          ) AS metadata,
          'trialing' AS status
          -- Leave `trial_end` and `trial_start` as is.
        )
    ) AS subscription,
    IF(
      subscription.metadata.previous_plan_id IS NOT NULL
      AND subscription.trial_start < subscription.metadata.plan_change_date,
      subscription.metadata.previous_plan_id,
      subscription_plan_id
    ) AS subscription_plan_id
  FROM
    questionable_resync_changelog
  WHERE
    subscription.trial_start > subscription.start_date
),
synthetic_trial_end_changelog AS (
  SELECT
    'synthetic_trial_end' AS type,
    original_id,
    subscription.trial_end AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          IF(
            subscription.cancel_at_period_end
            AND subscription.canceled_at <= subscription.trial_end,
            subscription.cancel_at,
            NULL
          ) AS cancel_at,
          IF(
            subscription.cancel_at_period_end
            AND subscription.canceled_at <= subscription.trial_end,
            TRUE,
            FALSE
          ) AS cancel_at_period_end,
          IF(
            subscription.canceled_at <= subscription.trial_end,
            subscription.canceled_at,
            NULL
          ) AS canceled_at,
          IF(
            subscription.current_period_start <= subscription.trial_end,
            subscription.current_period_end,
            NULL
          ) AS current_period_end,
          IF(
            subscription.current_period_start <= subscription.trial_end,
            subscription.current_period_start,
            NULL
          ) AS current_period_start,
          IF(
            subscription.discount.start <= subscription.trial_end,
            subscription.discount,
            NULL
          ) AS discount,
          CAST(NULL AS TIMESTAMP) AS ended_at,
          IF(
            subscription.current_period_start <= subscription.trial_end,
            subscription.latest_invoice_id,
            NULL
          ) AS latest_invoice_id,
          STRUCT(
            subscription.metadata.appliedPromotionCode,
            IF(
              subscription.metadata.cancelled_for_customer_at <= subscription.trial_end,
              subscription.metadata.cancelled_for_customer_at,
              NULL
            ) AS cancelled_for_customer_at,
            IF(
              subscription.metadata.plan_change_date <= subscription.trial_end,
              subscription.metadata.plan_change_date,
              NULL
            ) AS plan_change_date,
            IF(
              subscription.metadata.plan_change_date <= subscription.trial_end,
              subscription.metadata.previous_plan_id,
              NULL
            ) AS previous_plan_id
          ) AS metadata,
          'active' AS status
          -- Leave `trial_end` and `trial_start` as is.
        )
    ) AS subscription,
    IF(
      subscription.metadata.previous_plan_id IS NOT NULL
      AND subscription.trial_end < subscription.metadata.plan_change_date,
      subscription.metadata.previous_plan_id,
      subscription_plan_id
    ) AS subscription_plan_id
  FROM
    questionable_resync_changelog
  WHERE
    subscription.trial_end < synced_at
    -- Only consider the subscription active after the trial ended if it continued for at least a day.
    AND (
      subscription.ended_at IS NULL
      OR TIMESTAMP_DIFF(subscription.ended_at, subscription.trial_end, HOUR) >= 24
    )
),
synthetic_cancel_at_period_end_changelog AS (
  SELECT
    'synthetic_cancel_at_period_end' AS type,
    original_id,
    subscription.canceled_at AS `timestamp`,
    (
      SELECT AS STRUCT
        subscription.* REPLACE (
          -- Leave `cancel_at`, `cancel_at_period_end`, and `canceled_at` as is.
          IF(
            subscription.current_period_start <= subscription.canceled_at,
            subscription.current_period_end,
            NULL
          ) AS current_period_end,
          IF(
            subscription.current_period_start <= subscription.canceled_at,
            subscription.current_period_start,
            NULL
          ) AS current_period_start,
          IF(
            subscription.discount.start <= subscription.canceled_at,
            subscription.discount,
            NULL
          ) AS discount,
          CAST(NULL AS TIMESTAMP) AS ended_at,
          IF(
            subscription.current_period_start <= subscription.canceled_at,
            subscription.latest_invoice_id,
            NULL
          ) AS latest_invoice_id,
          STRUCT(
            subscription.metadata.appliedPromotionCode,
            IF(
              subscription.metadata.cancelled_for_customer_at <= subscription.canceled_at,
              subscription.metadata.cancelled_for_customer_at,
              NULL
            ) AS cancelled_for_customer_at,
            IF(
              subscription.metadata.plan_change_date <= subscription.canceled_at,
              subscription.metadata.plan_change_date,
              NULL
            ) AS plan_change_date,
            IF(
              subscription.metadata.plan_change_date <= subscription.canceled_at,
              subscription.metadata.previous_plan_id,
              NULL
            ) AS previous_plan_id
          ) AS metadata,
          IF(
            subscription.canceled_at >= subscription.trial_start
            AND subscription.canceled_at < subscription.trial_end,
            'trialing',
            'active'
          ) AS status,
          IF(
            subscription.trial_start <= subscription.canceled_at,
            subscription.trial_end,
            NULL
          ) AS trial_end,
          IF(
            subscription.trial_start <= subscription.canceled_at,
            subscription.trial_start,
            NULL
          ) AS trial_start
        )
    ) AS subscription,
    IF(
      subscription.metadata.previous_plan_id IS NOT NULL
      AND subscription.canceled_at < subscription.metadata.plan_change_date,
      subscription.metadata.previous_plan_id,
      subscription_plan_id
    ) AS subscription_plan_id
  FROM
    questionable_resync_changelog
  WHERE
    subscription.cancel_at_period_end
    AND subscription.canceled_at IS NOT NULL
    AND (subscription.ended_at IS NULL OR subscription.canceled_at < subscription.ended_at)
    -- Don't adjust questionable resync changes that would conflict with subsequent actual changes.
    AND (
      next_subscription_change_at IS NULL
      OR subscription.canceled_at < next_subscription_change_at
    )
),
synthetic_changelog_union AS (
  SELECT
    *
  FROM
    synthetic_subscription_start_changelog
  UNION ALL
  SELECT
    *
  FROM
    synthetic_plan_change_changelog
  UNION ALL
  SELECT
    *
  FROM
    synthetic_trial_start_changelog
  UNION ALL
  SELECT
    *
  FROM
    synthetic_trial_end_changelog
  UNION ALL
  SELECT
    *
  FROM
    synthetic_cancel_at_period_end_changelog
),
synthetic_changelog_with_plans AS (
  SELECT
    changelog.type,
    changelog.original_id,
    changelog.`timestamp`,
    (
      SELECT AS STRUCT
        changelog.subscription.* REPLACE (
          IF(
            changelog.subscription_plan_id = changelog.subscription.items[0].plan.id,
            changelog.subscription.items,
            [
              STRUCT(
                changelog.subscription.items[0].id,
                changelog.subscription.items[0].created,
                changelog.subscription.items[0].metadata,
                STRUCT(
                  changelog.subscription_plan_id AS id,
                  plans.aggregate_usage,
                  plans.amount,
                  plans.billing_scheme,
                  plans.created,
                  plans.currency,
                  plans.`interval`,
                  plans.interval_count,
                  PARSE_JSON(plans.metadata) AS metadata,
                  plans.nickname,
                  plans.product_id,
                  plans.tiers_mode,
                  plans.trial_period_days,
                  plans.usage_type
                ) AS plan,
                changelog.subscription.items[0].quantity
              )
            ]
          ) AS items
        )
    ) AS subscription
  FROM
    synthetic_changelog_union AS changelog
  LEFT JOIN
    `moz-fx-data-shared-prod`.stripe_external.plan_v1 AS plans
  ON
    changelog.subscription_plan_id = plans.id
),
adjusted_resync_changelog AS (
  SELECT
    'adjusted_resync' AS type,
    original_id,
    COALESCE(subscription.ended_at, synced_at) AS `timestamp`,
    subscription
  FROM
    questionable_resync_changelog
  WHERE
    -- Don't adjust questionable resync changes that would conflict with subsequent actual changes.
    next_subscription_change_at IS NULL
    OR COALESCE(subscription.ended_at, synced_at) < next_subscription_change_at
),
changelog_union AS (
  SELECT
    type,
    original_id,
    `timestamp`,
    subscription
  FROM
    adjusted_original_changelog
  WHERE
    NOT ((DATE(synced_at) BETWEEN '2023-02-24' AND '2023-02-26') AND subscription_change_number = 1)
  UNION ALL
  SELECT
    type,
    original_id,
    `timestamp`,
    subscription
  FROM
    synthetic_changelog_with_plans
  UNION ALL
  SELECT
    type,
    original_id,
    `timestamp`,
    subscription
  FROM
    adjusted_resync_changelog
)
SELECT
  CONCAT(subscription.id, '-', FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`)) AS id,
  `timestamp`,
  type,
  original_id AS stripe_subscriptions_raw_changelog_id,
  subscription
FROM
  changelog_union
WHERE
  DATE(`timestamp`) = @date
