WITH flows_live AS (
  SELECT
    *
  FROM
    `moz-fx-cjms-nonprod-9a36`.cjms_bigquery.flows_v1
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-cjms-nonprod-9a36`.cjms_bigquery.flows_live
  WHERE
    submission_date = CURRENT_DATE
),
aic_flows AS (
  -- last fxa_uid for each flow_id
  SELECT
    flow_id,
    MIN(flow_started) AS flow_started,
    ARRAY_AGG(fxa_uid IGNORE NULLS ORDER BY fxa_uid_timestamp DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS fxa_uid,
  FROM
    flows_live
  JOIN
    EXTERNAL_QUERY("moz-fx-cjms-nonprod-9a36.us.cjms-sql", "SELECT flow_id FROM aic")
  USING
    (flow_id)
  WHERE
    -- only use the last 10 days in stage
    submission_date >= CURRENT_DATE - 10
  GROUP BY
    flow_id
),
attributed_flows AS (
  -- last flow that started before subscription created
  SELECT
    nonprod_stripe_subscriptions.subscription_id,
    nonprod_stripe_subscriptions.created AS subscription_created,
    ARRAY_AGG(aic_flows.flow_id ORDER BY aic_flows.flow_started DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS flow_id,
  FROM
    aic_flows
  JOIN
    mozdata.subscription_platform.nonprod_stripe_subscriptions
  ON
    aic_flows.fxa_uid = nonprod_stripe_subscriptions.fxa_uid
    AND aic_flows.flow_started < nonprod_stripe_subscriptions.created
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
  nonprod_stripe_subscriptions.created AS subscription_created,
  attributed_subs.subscription_id, -- transaction id
  nonprod_stripe_subscriptions.fxa_uid,
  1 AS quantity,
  nonprod_stripe_subscriptions.plan_id, -- sku
  nonprod_stripe_subscriptions.plan_currency,
  nonprod_stripe_subscriptions.plan_amount,
  nonprod_stripe_subscriptions.country,
  attributed_subs.flow_id,
FROM
  attributed_subs
JOIN
  mozdata.subscription_platform.nonprod_stripe_subscriptions
USING
  (subscription_id)
