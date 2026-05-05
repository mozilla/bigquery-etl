WITH stripe_subscriptions_history AS (
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
      trial_start,
      COALESCE(plan_started_at, subscription_start_date)
    ) AS effective_start_date,
    attribution
  FROM
    stripe_subscriptions_history
  WHERE
    "guardian_vpn_1" IN UNNEST(stripe_subscriptions_history.product_capabilities)
    OR "guardian_vpn_1" IN UNNEST(stripe_subscriptions_history.plan_capabilities)
)
SELECT
  subscription_id,
  DATE(effective_start_date) AS `date`,
  STRUCT(
    effective_start_date AS `timestamp`,
    attribution.entrypoint_experiment,
    attribution.entrypoint_variation,
    attribution.utm_campaign,
    attribution.utm_content,
    attribution.utm_medium,
    attribution.utm_source,
    attribution.utm_term
  ) AS attribution
FROM
  stripe_subscriptions
WHERE
  attribution IS NOT NULL
  {% if is_init() %}
    AND DATE(effective_start_date) < CURRENT_DATE()
  {% else %}
    AND DATE(effective_start_date) = @date
  {% endif %}
