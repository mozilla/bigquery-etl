CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.subscription_transitions`
AS
WITH subscriptions AS (
  SELECT
    products_v1.name AS product,
    (
      cancel_at_period_end
      OR "cancelled_for_customer_at" IN (SELECT key FROM UNNEST(subscriptions_v1.metadata))
    ) AS cancelled_for_customer,
    DATE(start_date) AS start_date,
    DATE(trial_end) AS trial_end,
    DATE(ended_at) AS end_date,
    MIN(DATE(start_date)) OVER (PARTITION BY customer) AS customer_start_date,
    customer,
  FROM
    `moz-fx-data-shared-prod`.stripe_derived.subscriptions_v1
  LEFT JOIN
    `moz-fx-data-shared-prod`.stripe_derived.plans_v1
  ON
    (subscriptions_v1.plan = plans_v1.id)
  LEFT JOIN
    `moz-fx-data-shared-prod`.stripe_derived.products_v1
  ON
    (plans_v1.product = products_v1.id)
  WHERE
    status NOT IN ("incomplete", "incomplete_expired")
),
counts AS (
  SELECT
    `date`,
    product,
    transition,
    -- make count negative for subscription ended counts
    IF(
      transition IN ("trial cancelled", "trial convert failed", "cancelled", "renew failed"),
      -1,
      1
    ) * COUNT(*) AS subscription_count,
  FROM
    subscriptions
  CROSS JOIN
    UNNEST(
      ARRAY[
        STRUCT(
          start_date AS date,
          IF(
            trial_end IS NOT NULL,
            IF(start_date > customer_start_date, "resurrected trial", "new trial"),
            IF(start_date > customer_start_date, "resurrected", "new")
          ) AS transition
        ),
        STRUCT(
          IF(trial_end < COALESCE(end_date, CURRENT_DATE), trial_end, NULL) AS date,
          "converted" AS transition
        ),
        STRUCT(
          end_date AS date,
          IF(
            end_date <= trial_end,
            IF(cancelled_for_customer, "trial cancelled", "trial convert failed"),
            IF(cancelled_for_customer, "cancelled", "renew failed")
          ) AS transition
        )
      ]
    )
  WHERE
    `date` IS NOT NULL
  GROUP BY
    `date`,
    product,
    transition
),
fill_groups AS (
  SELECT
    `date`,
    product,
    transition
  FROM
    (SELECT DISTINCT `date`, product FROM counts)
  JOIN
    (SELECT DISTINCT transition, product FROM counts)
  USING
    (product)
)
SELECT
  `date`,
  product,
  transition,
  COALESCE(subscription_count, 0) AS subscription_count,
FROM
  counts
-- join on every combination of date/transition for each product to fill zeroes for subscription_count,
-- to make it easier to control graphing order in re:dash visualizations
FULL JOIN
  fill_groups
USING
  (`date`, product, transition)
