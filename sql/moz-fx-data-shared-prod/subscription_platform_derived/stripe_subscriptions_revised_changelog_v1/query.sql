CREATE TEMP FUNCTION synthesize_subscription(
  subscription ANY TYPE,
  effective_at TIMESTAMP,
  plan ANY TYPE,
  plan_start TIMESTAMP,
  previous_plan_id STRING
) AS (
  -- Generate a synthetic subscription record from an existing subscription record
  -- to simulate how the subscription was at the specified `effective_at` timestamp,
  -- which is presumed to be before the subscription ended.
  (
    SELECT AS STRUCT
      subscription.* REPLACE (
        IF(
          subscription.billing_cycle_anchor <= effective_at,
          subscription.billing_cycle_anchor,
          NULL
        ) AS billing_cycle_anchor,
        IF(
          subscription.cancel_at_period_end
          AND subscription.canceled_at <= effective_at,
          subscription.cancel_at,
          NULL
        ) AS cancel_at,
        IF(
          subscription.cancel_at_period_end
          AND subscription.canceled_at <= effective_at,
          TRUE,
          FALSE
        ) AS cancel_at_period_end,
        IF(subscription.canceled_at <= effective_at, subscription.canceled_at, NULL) AS canceled_at,
        IF(
          subscription.current_period_start <= effective_at,
          subscription.current_period_end,
          NULL
        ) AS current_period_end,
        IF(
          subscription.current_period_start <= effective_at,
          subscription.current_period_start,
          NULL
        ) AS current_period_start,
        ARRAY(
          SELECT AS STRUCT
            *
          FROM
            UNNEST(subscription.default_tax_rates) AS default_tax_rates
          WHERE
            default_tax_rates.created <= effective_at
        ) AS default_tax_rates,
        IF(subscription.discount.start <= effective_at, subscription.discount, NULL) AS discount,
        CAST(NULL AS TIMESTAMP) AS ended_at,
        IF(
          subscription.items[0].plan.id = plan.id,
          subscription.items,
          [(SELECT AS STRUCT subscription.items[0].* REPLACE (plan AS plan))]
        ) AS items,
        IF(
          subscription.current_period_start <= effective_at,
          subscription.latest_invoice_id,
          NULL
        ) AS latest_invoice_id,
        STRUCT(
          subscription.metadata.appliedPromotionCode,
          IF(
            subscription.metadata.cancelled_for_customer_at <= effective_at,
            subscription.metadata.cancelled_for_customer_at,
            NULL
          ) AS cancelled_for_customer_at,
          IF(plan_start > subscription.start_date, plan_start, NULL) AS plan_change_date,
          previous_plan_id
        ) AS metadata,
        CASE
          WHEN subscription.status IN ('incomplete', 'incomplete_expired')
            THEN 'incomplete'
          WHEN effective_at >= subscription.trial_start
            AND effective_at < subscription.trial_end
            THEN 'trialing'
          -- Only consider canceled subscriptions to have been active if they lasted for at least a day.
          WHEN subscription.status = 'canceled'
            AND TIMESTAMP_DIFF(subscription.ended_at, subscription.start_date, HOUR) < 24
            THEN 'incomplete'
          ELSE 'active'
        END AS status,
        IF(subscription.trial_start <= effective_at, subscription.trial_end, NULL) AS trial_end,
        IF(subscription.trial_start <= effective_at, subscription.trial_start, NULL) AS trial_start
      )
  )
);

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
    ROW_NUMBER() OVER subscription_changes_asc AS subscription_change_number,
    LEAD(`timestamp`) OVER subscription_changes_asc AS next_subscription_change_at,
    LAG(subscription.ended_at) OVER subscription_changes_asc AS previous_subscription_ended_at
  FROM
    `moz-fx-data-shared-prod`.stripe_external.subscriptions_changelog_v1
  WINDOW
    subscription_changes_asc AS (
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
        ELSE STRUCT(`timestamp`, 'original' AS type)
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
    *
  FROM
    adjusted_original_changelog
  WHERE
    (DATE(synced_at) BETWEEN '2023-02-24' AND '2023-02-26')
    AND subscription_change_number = 1
),
questionable_subscription_plan_changes AS (
  -- The most recent plan changes at the time of the resync.
  SELECT
    subscription.id AS subscription_id,
    subscription.items[0].plan.id AS plan_id,
    COALESCE(
      subscription.metadata.plan_change_date,
      subscription.start_date
    ) AS subscription_plan_start
  FROM
    questionable_resync_changelog
  UNION ALL
  -- Any additional previous plan changes.
  SELECT
    invoice_line_items.subscription_id,
    invoice_line_items.plan_id,
    COALESCE(
      TIMESTAMP_SECONDS(
        CAST(JSON_VALUE(invoice_line_items.metadata, '$.plan_change_date') AS INT64)
      ),
      invoice_line_items.period_start
    ) AS subscription_plan_start
  FROM
    questionable_resync_changelog AS changelog
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.invoice_line_item_v1 AS invoice_line_items
  ON
    changelog.subscription.id = invoice_line_items.subscription_id
    AND invoice_line_items.type = 'subscription'
    AND invoice_line_items.period_start < changelog.subscription.metadata.plan_change_date
  WHERE
    changelog.subscription.metadata.plan_change_date IS NOT NULL
  QUALIFY
    invoice_line_items.plan_id IS DISTINCT FROM LAG(invoice_line_items.plan_id) OVER (
      PARTITION BY
        invoice_line_items.subscription_id
      ORDER BY
        invoice_line_items.period_start
    )
),
questionable_subscription_plans_history AS (
  SELECT
    plan_changes.subscription_id,
    STRUCT(
      plan_changes.plan_id AS id,
      plans.aggregate_usage,
      plans.amount,
      plans.billing_scheme,
      plans.created,
      plans.currency,
      plans.`interval`,
      plans.interval_count,
      plans.metadata,
      plans.nickname,
      STRUCT(
        plans.product_id AS id,
        products.created,
        products.description,
        products.metadata,
        products.name,
        products.statement_descriptor,
        products.updated
      ) AS product,
      plans.tiers_mode,
      plans.trial_period_days,
      plans.usage_type
    ) AS plan,
    ROW_NUMBER() OVER subscription_plan_changes_asc AS subscription_plan_number,
    LAG(plan_changes.plan_id) OVER subscription_plan_changes_asc AS previous_plan_id,
    plan_changes.subscription_plan_start AS valid_from,
    COALESCE(
      LEAD(plan_changes.subscription_plan_start) OVER subscription_plan_changes_asc,
      '9999-12-31 23:59:59.999999'
    ) AS valid_to
  FROM
    questionable_subscription_plan_changes AS plan_changes
  LEFT JOIN
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_plans_v1 AS plans
  ON
    plan_changes.plan_id = plans.id
  LEFT JOIN
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_products_v1 AS products
  ON
    plans.product_id = products.id
  WINDOW
    subscription_plan_changes_asc AS (
      PARTITION BY
        plan_changes.subscription_id
      ORDER BY
        plan_changes.subscription_plan_start
    )
),
synthetic_subscription_start_changelog AS (
  SELECT
    'synthetic_subscription_start' AS type,
    changelog.original_id,
    changelog.`timestamp`,
    synthesize_subscription(
      changelog.subscription,
      effective_at => changelog.subscription.start_date,
      plan => plans_history.plan,
      plan_start => changelog.subscription.start_date,
      previous_plan_id => CAST(NULL AS STRING)
    ) AS subscription
  FROM
    questionable_resync_changelog AS changelog
  LEFT JOIN
    questionable_subscription_plans_history AS plans_history
  ON
    changelog.subscription.id = plans_history.subscription_id
    AND plans_history.subscription_plan_number = 1
),
synthetic_plan_change_changelog AS (
  SELECT
    'synthetic_plan_change' AS type,
    changelog.original_id,
    plans_history.valid_from AS `timestamp`,
    synthesize_subscription(
      changelog.subscription,
      effective_at => plans_history.valid_from,
      plan => plans_history.plan,
      plan_start => plans_history.valid_from,
      previous_plan_id => plans_history.previous_plan_id
    ) AS subscription
  FROM
    questionable_subscription_plans_history AS plans_history
  JOIN
    questionable_resync_changelog AS changelog
  ON
    plans_history.subscription_id = changelog.subscription.id
  WHERE
    plans_history.subscription_plan_number > 1
    AND plans_history.valid_from > changelog.subscription.start_date
),
synthetic_trial_start_changelog AS (
  SELECT
    'synthetic_trial_start' AS type,
    changelog.original_id,
    changelog.subscription.trial_start AS `timestamp`,
    synthesize_subscription(
      changelog.subscription,
      effective_at => changelog.subscription.trial_start,
      plan => plans_history.plan,
      plan_start => plans_history.valid_from,
      previous_plan_id => plans_history.previous_plan_id
    ) AS subscription
  FROM
    questionable_resync_changelog AS changelog
  LEFT JOIN
    questionable_subscription_plans_history AS plans_history
  ON
    changelog.subscription.id = plans_history.subscription_id
    AND changelog.subscription.trial_start >= plans_history.valid_from
    AND changelog.subscription.trial_start < plans_history.valid_to
  WHERE
    changelog.subscription.trial_start > changelog.subscription.start_date
),
synthetic_trial_end_changelog AS (
  SELECT
    'synthetic_trial_end' AS type,
    changelog.original_id,
    changelog.subscription.trial_end AS `timestamp`,
    synthesize_subscription(
      changelog.subscription,
      effective_at => changelog.subscription.trial_end,
      plan => plans_history.plan,
      plan_start => plans_history.valid_from,
      previous_plan_id => plans_history.previous_plan_id
    ) AS subscription
  FROM
    questionable_resync_changelog AS changelog
  LEFT JOIN
    questionable_subscription_plans_history AS plans_history
  ON
    changelog.subscription.id = plans_history.subscription_id
    AND changelog.subscription.trial_end >= plans_history.valid_from
    AND changelog.subscription.trial_end < plans_history.valid_to
  WHERE
    changelog.subscription.trial_end < changelog.synced_at
    -- Only consider the subscription active after the trial ended if it continued for at least a day.
    AND (
      changelog.subscription.ended_at IS NULL
      OR TIMESTAMP_DIFF(
        changelog.subscription.ended_at,
        changelog.subscription.trial_end,
        HOUR
      ) >= 24
    )
),
synthetic_cancel_at_period_end_changelog AS (
  SELECT
    'synthetic_cancel_at_period_end' AS type,
    changelog.original_id,
    changelog.subscription.canceled_at AS `timestamp`,
    synthesize_subscription(
      changelog.subscription,
      effective_at => changelog.subscription.canceled_at,
      plan => plans_history.plan,
      plan_start => plans_history.valid_from,
      previous_plan_id => plans_history.previous_plan_id
    ) AS subscription
  FROM
    questionable_resync_changelog AS changelog
  LEFT JOIN
    questionable_subscription_plans_history AS plans_history
  ON
    changelog.subscription.id = plans_history.subscription_id
    AND changelog.subscription.canceled_at >= plans_history.valid_from
    AND changelog.subscription.canceled_at < plans_history.valid_to
  WHERE
    changelog.subscription.cancel_at_period_end
    AND changelog.subscription.canceled_at IS NOT NULL
    AND (
      changelog.subscription.ended_at IS NULL
      OR changelog.subscription.canceled_at < changelog.subscription.ended_at
    )
    -- Don't adjust questionable resync changes that would conflict with subsequent actual changes.
    AND (
      changelog.next_subscription_change_at IS NULL
      OR changelog.subscription.canceled_at < changelog.next_subscription_change_at
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
adjusted_resync_changelog AS (
  SELECT
    'adjusted_resync' AS type,
    original_id,
    -- For canceled subscriptions adjust the questionable resync change to represent when the subscription ended.
    -- For all other subscriptions adjust the questionable resync change to the time of the resync.
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
    synthetic_changelog_union
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
  original_id AS stripe_subscriptions_changelog_id,
  subscription
FROM
  changelog_union
WHERE
  DATE(`timestamp`) = @date
