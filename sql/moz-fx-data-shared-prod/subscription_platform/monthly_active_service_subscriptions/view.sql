CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.monthly_active_service_subscriptions`
AS
WITH monthly_subscriptions AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_service_subscriptions_v1`
  WHERE
    month_start_date < (
      SELECT
        COALESCE(MIN(month_start_date), '9999-12-31')
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.recent_monthly_active_service_subscriptions_v1`
    )
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.recent_monthly_active_service_subscriptions_v1`
),
augmented_monthly_subscriptions AS (
  SELECT
    *,
    COALESCE(DATE(subscription.ended_at), LEAST(month_end_date, CURRENT_DATE())) AS effective_date
  FROM
    monthly_subscriptions
),
augmented_monthly_subscriptions_2 AS (
  SELECT
    monthly_subscriptions.*,
    vat_rates.vat_rate AS country_vat_rate,
    usd_exchange_rates.exchange_rate AS plan_currency_usd_exchange_rate,
  FROM
    augmented_monthly_subscriptions AS monthly_subscriptions
  LEFT JOIN
    `moz-fx-data-shared-prod.subscription_platform.vat_rates_history` AS vat_rates
    ON monthly_subscriptions.subscription.country_code = vat_rates.country_code
    AND (monthly_subscriptions.effective_date BETWEEN vat_rates.valid_from AND vat_rates.valid_to)
  LEFT JOIN
    `moz-fx-data-shared-prod.subscription_platform.exchange_rates_history` AS usd_exchange_rates
    ON monthly_subscriptions.subscription.plan_currency = usd_exchange_rates.base_currency
    AND usd_exchange_rates.quote_currency = 'USD'
    AND (
      monthly_subscriptions.effective_date
      BETWEEN usd_exchange_rates.valid_from
      AND usd_exchange_rates.valid_to
    )
),
augmented_monthly_subscriptions_3 AS (
  SELECT
    *,
    LEAST(
      (
        mozfun.utils.timestamp_diff_complete_months(
          subscription.current_period_ends_at,
          LEAST(
            TIMESTAMP_SUB(TIMESTAMP(month_end_date + 1), INTERVAL 1 MICROSECOND),
            TIMESTAMP(CURRENT_DATE()),
            subscription.current_period_ends_at
          )
        ) + 1
      ),
      12
    ) AS current_period_annual_recurring_revenue_months,
    GREATEST(
      mozfun.utils.timestamp_diff_complete_months(
        TIMESTAMP_SUB(subscription.ongoing_discount_ends_at, INTERVAL 1 MICROSECOND),
        subscription.current_period_ends_at
      ),
      0
    ) AS months_after_current_period_before_ongoing_discount_ends
  FROM
    augmented_monthly_subscriptions_2
),
augmented_monthly_subscriptions_4 AS (
  SELECT
    *,
    CASE
      WHEN COALESCE(subscription.ongoing_discount_amount, 0) = 0
        THEN 0
      WHEN subscription.ongoing_discount_ends_at IS NULL
        THEN (12 - current_period_annual_recurring_revenue_months)
      ELSE LEAST(
          (
            (
              DIV(
                months_after_current_period_before_ongoing_discount_ends,
                subscription.plan_interval_months
              ) + 1
            ) * subscription.plan_interval_months
          ),
          (12 - current_period_annual_recurring_revenue_months)
        )
    END AS ongoing_discounted_annual_recurring_revenue_months
  FROM
    augmented_monthly_subscriptions_3
),
augmented_monthly_subscriptions_5 AS (
  SELECT
    *,
    IF(
      subscription.is_active IS NOT TRUE
      OR subscription.is_trial IS TRUE,
      0,
      (
        GREATEST(
          (subscription.plan_amount - COALESCE(subscription.current_period_discount_amount, 0)),
          0
        ) / subscription.plan_interval_months
      )
    ) AS monthly_recurring_gross_revenue,
    IF(
      subscription.is_active IS NOT TRUE,
      0,
      (
        -- Current period annual recurring gross revenue
        IF(
          subscription.is_trial IS TRUE,
          0,
          (
            GREATEST(
              (subscription.plan_amount - COALESCE(subscription.current_period_discount_amount, 0)),
              0
            ) / subscription.plan_interval_months * current_period_annual_recurring_revenue_months
          )
        )
        -- Ongoing discounted annual recurring gross revenue
        + IF(
          subscription.auto_renew IS NOT TRUE
          OR COALESCE(subscription.ongoing_discount_amount, 0) = 0,
          0,
          (
            GREATEST(
              (subscription.plan_amount - COALESCE(subscription.ongoing_discount_amount, 0)),
              0
            ) / subscription.plan_interval_months * ongoing_discounted_annual_recurring_revenue_months
          )
        )
        -- Ongoing undiscounted annual recurring gross revenue
        + IF(
          subscription.auto_renew IS NOT TRUE,
          0,
          (
            subscription.plan_amount / subscription.plan_interval_months * GREATEST(
              (
                12 - current_period_annual_recurring_revenue_months - ongoing_discounted_annual_recurring_revenue_months
              ),
              0
            )
          )
        )
      )
    ) AS annual_recurring_gross_revenue
  FROM
    augmented_monthly_subscriptions_4
),
augmented_monthly_subscriptions_6 AS (
  SELECT
    *,
    ROUND(
      (
        (monthly_recurring_gross_revenue / (1 + COALESCE(country_vat_rate, 0)))
        -- Subtract service fees charged by Apple App Store and Google Play Store (DENG-9773).
        - IF(
          subscription.provider IN ('Apple', 'Google'),
          (monthly_recurring_gross_revenue * 0.15),
          0
        )
      ),
      2
    ) AS monthly_recurring_revenue,
    ROUND(
      (
        (annual_recurring_gross_revenue / (1 + COALESCE(country_vat_rate, 0)))
        -- Subtract service fees charged by Apple App Store and Google Play Store (DENG-9773).
        - IF(
          subscription.provider IN ('Apple', 'Google'),
          (annual_recurring_gross_revenue * 0.15),
          0
        )
      ),
      2
    ) AS annual_recurring_revenue
  FROM
    augmented_monthly_subscriptions_5
)
SELECT
  id,
  month_start_date,
  month_end_date,
  service_id,
  service_subscriptions_history_id,
  (
    SELECT AS STRUCT
      subscription.* EXCEPT (first_touch_attribution),  -- DENG-9858
      country_vat_rate,
      plan_currency_usd_exchange_rate,
      IF(
        subscription.plan_currency = 'USD',
        subscription.plan_amount,
        ROUND((subscription.plan_amount * plan_currency_usd_exchange_rate), 2)
      ) AS plan_amount_usd,
      IF(
        subscription.plan_currency = 'USD',
        subscription.current_period_discount_amount,
        ROUND((subscription.current_period_discount_amount * plan_currency_usd_exchange_rate), 2)
      ) AS current_period_discount_amount_usd,
      IF(
        subscription.plan_currency = 'USD',
        subscription.ongoing_discount_amount,
        ROUND((subscription.ongoing_discount_amount * plan_currency_usd_exchange_rate), 2)
      ) AS ongoing_discount_amount_usd,
      IF(
        subscription.plan_currency = 'USD',
        monthly_recurring_revenue,
        ROUND((monthly_recurring_revenue * plan_currency_usd_exchange_rate), 2)
      ) AS monthly_recurring_revenue_usd,
      IF(
        subscription.plan_currency = 'USD',
        annual_recurring_revenue,
        ROUND((annual_recurring_revenue * plan_currency_usd_exchange_rate), 2)
      ) AS annual_recurring_revenue_usd
  ) AS subscription,
  was_active_at_month_start,
  was_active_at_month_end
FROM
  augmented_monthly_subscriptions_6
