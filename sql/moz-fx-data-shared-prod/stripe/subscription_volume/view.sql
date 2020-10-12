CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod`.stripe.subscription_volume
AS
WITH subscriptions AS (
  SELECT
    DATE(start_date) AS start_date,
    DATE(trial_end) AS trial_end,
    COALESCE(DATE(ended_at), CURRENT_DATE) AS end_date,
    products_v1.name AS product,
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
)
SELECT
  `date`,
  product,
  COALESCE(`date` >= trial_end, TRUE) AS paying,
  COUNT(*) AS subscription_count,
FROM
  subscriptions
CROSS JOIN
  UNNEST(
    GENERATE_DATE_ARRAY(
      start_date,
      -- GENERATE_DATE_ARRAY is inclusive, so subtract one day to exclude end date
      DATE_SUB(end_date, INTERVAL 1 DAY)
    )
  ) AS `date`
GROUP BY
  `date`,
  product,
  paying
