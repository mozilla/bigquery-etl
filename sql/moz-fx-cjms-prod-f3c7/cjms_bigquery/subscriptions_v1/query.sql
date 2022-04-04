WITH aic_flows AS (
  -- last fxa_uid for each flow_id
  SELECT
    flow_id,
    MIN(flow_started) AS flow_started,
    ARRAY_AGG(fxa_uid IGNORE NULLS ORDER BY fxa_uid_timestamp DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS fxa_uid,
  FROM
    `moz-fx-cjms-prod-f3c7`.cjms_bigquery.flows_v1
  JOIN
    EXTERNAL_QUERY("moz-fx-cjms-prod-f3c7.us.cjms-sql", "SELECT flow_id FROM aic")
  USING
    (flow_id)
  WHERE
    -- use the last 31 days in prod
    submission_date >= CURRENT_DATE - 31
  GROUP BY
    flow_id
),
attributed_flows AS (
  -- last flow that started before subscription created
  SELECT
    stripe_subscriptions.subscription_id,
    stripe_subscriptions.created AS subscription_created,
    ARRAY_AGG(aic_flows.flow_id ORDER BY aic_flows.flow_started DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS flow_id,
  FROM
    aic_flows
  JOIN
    mozdata.subscription_platform.stripe_subscriptions
  ON
    aic_flows.fxa_uid = stripe_subscriptions.fxa_uid
    AND aic_flows.flow_started < stripe_subscriptions.created
  GROUP BY
    subscription_id,
    subscription_created
),
attributed_subs AS (
  -- first subscription for each flow, for 1:1 relationship between flow and subscription
  SELECT
    flow_id,
    ARRAY_AGG(subscription_id ORDER BY subscription_created LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS subscription_id,
  FROM
    attributed_flows
  GROUP BY
    flow_id
)
SELECT
  CURRENT_TIMESTAMP AS report_timestamp,
  stripe_subscriptions.created AS subscription_created,
  attributed_subs.subscription_id, -- transaction id
  stripe_subscriptions.fxa_uid,
  1 AS quantity,
  stripe_subscriptions.plan_id, -- sku
  stripe_subscriptions.plan_currency,
  stripe_subscriptions.plan_amount,
  stripe_subscriptions.country,
  attributed_subs.flow_id,
FROM
  attributed_subs
JOIN
  mozdata.subscription_platform.stripe_subscriptions
USING
  (subscription_id)
