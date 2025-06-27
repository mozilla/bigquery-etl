WITH fxa_attributions AS (
  SELECT
    fxa_uid,
    attribution
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.fxa_attribution_v1
  CROSS JOIN
    UNNEST(fxa_uids) AS fxa_uid
  WHERE
    attribution IS NOT NULL
),
stripe_subscriptions_history AS (
  SELECT
    *,
    CONCAT(
      subscription_id,
      COALESCE(
        CONCAT(
          "-",
          NULLIF(ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY valid_from), 1)
        ),
        ""
      )
    ) AS subscription_sequence_id
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_subscriptions_history_v1
  WHERE
    -- Only include the current history records and the last history records for previous plans.
    (valid_to IS NULL OR plan_ended_at IS NOT NULL)
    AND status NOT IN ("incomplete", "incomplete_expired")
),
stripe_subscriptions AS (
  SELECT
    subscription_sequence_id AS subscription_id,
    IF(
      (trial_end > TIMESTAMP(CURRENT_DATE) OR ended_at <= trial_end),
      NULL,
      COALESCE(plan_started_at, subscription_start_date)
    ) AS subscription_start_date,
    --first subscription start date associated with the subscription id
    IF(
      (trial_end > TIMESTAMP(CURRENT_DATE) OR ended_at <= trial_end),
      NULL,
      subscription_start_date
    ) AS original_subscription_start_date,
    trial_start,
    fxa_uid,
  FROM
    stripe_subscriptions_history
  WHERE
    "guardian_vpn_1" IN UNNEST(stripe_subscriptions_history.product_capabilities)
    OR "guardian_vpn_1" IN UNNEST(stripe_subscriptions_history.plan_capabilities)
),
apple_iap_subscriptions AS (
  SELECT
    subplat.subscription_id,
    subplat.subscription_start_date,
    -- Until the upgrade event surfacing work, original_subscription_start_date is set to be NULL
    CAST(NULL AS TIMESTAMP) AS original_subscription_start_date,
    subplat.trial_start,
    subplat.fxa_uid,
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.apple_subscriptions_v1 AS subplat
  WHERE
    subplat.product_id = "org.mozilla.ios.FirefoxVPN"
    AND subplat.fxa_uid IS NOT NULL
),
google_iap_subscriptions AS (
  SELECT
    subscriptions.subscription_id,
    subscriptions.subscription_start AS subscription_start_date,
    -- Until the upgrade event surfacing work, original_subscription_start_date is set to be NULL
    CAST(NULL AS TIMESTAMP) AS original_subscription_start_date,
    subscriptions.trial_start,
    subscriptions.fxa_uid,
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.google_subscriptions_v1 AS subscriptions
  WHERE
    subscriptions.product_id = "org.mozilla.firefox.vpn"
),
all_subscriptions AS (
  SELECT
    *
  FROM
    stripe_subscriptions
  UNION ALL
  SELECT
    *
  FROM
    apple_iap_subscriptions
  UNION ALL
  SELECT
    *
  FROM
    google_iap_subscriptions
),
all_subscriptions_with_effective_start_date AS (
  SELECT
    *,
    COALESCE(
      original_subscription_start_date,
      subscription_start_date,
      trial_start
    ) AS effective_start_date
  FROM
    all_subscriptions
)
-- Select the latest attribution before the subscription originally started.
SELECT
  subscriptions.subscription_id,
  DATE(subscriptions.effective_start_date) AS `date`,
  ARRAY_AGG(
    fxa_attributions.attribution
    ORDER BY
      fxa_attributions.attribution.timestamp DESC
    LIMIT
      1
  )[ORDINAL(1)] AS attribution
FROM
  all_subscriptions_with_effective_start_date AS subscriptions
JOIN
  fxa_attributions
  ON subscriptions.fxa_uid = fxa_attributions.fxa_uid
  AND subscriptions.effective_start_date >= fxa_attributions.attribution.timestamp
WHERE
  {% if is_init() %}
    DATE(subscriptions.effective_start_date) < CURRENT_DATE()
  {% else %}
    DATE(subscriptions.effective_start_date) = @date
  {% endif %}
GROUP BY
  subscription_id,
  `date`
