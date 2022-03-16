WITH flows AS (
  -- last fxa_uid for each flow_id
  SELECT
    flow_id,
    MIN(flow_started) AS flow_started,
    ARRAY_AGG(fxa_uid IGNORE NULLS ORDER BY fxa_uid_timestamp DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS fxa_uid,
  FROM
    `moz-fx-cjms-nonprod-9a36`.cjms_bigquery.flows_v1
  WHERE
    -- only use the last 10 days in stage
    submission_date >= CURRENT_DATE - 10
  GROUP BY
    flow_id
),
attributed_flows AS (
  -- last flow_id in aic for each fxa_uid
  SELECT
    fxa_uid,
    ARRAY_AGG(STRUCT(flow_id, flow_started) ORDER BY flow_started DESC LIMIT 1)[SAFE_OFFSET(0)].*,
  FROM
    flows
  JOIN
    EXTERNAL_QUERY("moz-fx-cjms-nonprod-9a36.us.cjms-sql", "SELECT flow_id FROM aic")
  USING
    (flow_id)
  GROUP BY
    fxa_uid
),
attributed_subs AS (
  -- last subscription_id created after the flow started for each fxa_uid
  SELECT
    attributed_flows.fxa_uid,
    ARRAY_AGG(
      nonprod_stripe_subscriptions.subscription_id
      ORDER BY
        nonprod_stripe_subscriptions.created DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS subscription_id,
  FROM
    attributed_flows
  JOIN
    mozdata.subscription_platform.nonprod_stripe_subscriptions
  ON
    attributed_flows.fxa_uid = nonprod_stripe_subscriptions.fxa_uid
    AND attributed_flows.flow_started < nonprod_stripe_subscriptions.created
  GROUP BY
    fxa_uid
)
SELECT
  CURRENT_TIMESTAMP AS report_timestamp,
  created AS subscription_created,
  subscription_id, -- transaction id
  fxa_uid,
  1 AS quantity,
  plan_id, -- sku
  plan_currency,
  plan_amount,
  country,
  flow_id,
FROM
  attributed_subs
JOIN
  attributed_flows
USING
  (fxa_uid)
JOIN
  mozdata.subscription_platform.nonprod_stripe_subscriptions
USING
  (fxa_uid, subscription_id)
