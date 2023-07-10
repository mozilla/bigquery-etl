CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn_derived.channel_group_proportions_live`
AS
WITH stage_1 AS (
  SELECT
    event_date AS subscription_start_date,
    country,
    country_name,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    entrypoint_experiment,
    entrypoint_variation,
    product_name,
    pricing_plan,
    plan_interval,
    plan_interval_count,
    provider,
    TO_JSON_STRING(promotion_codes) AS json_promotion_codes,
    granular_event_type,
    SUM(`count`) AS new_subscriptions,
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn.subscription_events
  WHERE
    event_type = "New"
  GROUP BY
    subscription_start_date,
    country,
    country_name,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    entrypoint_experiment,
    entrypoint_variation,
    product_name,
    pricing_plan,
    plan_interval,
    plan_interval_count,
    provider,
    json_promotion_codes,
    granular_event_type
),
stage_2 AS (
  SELECT
    * EXCEPT (json_promotion_codes),
    JSON_VALUE_ARRAY(json_promotion_codes) AS promotion_codes,
    mozfun.vpn.channel_group(
      utm_campaign => utm_campaign,
      utm_content => utm_content,
      utm_medium => utm_medium,
      utm_source => utm_source
    ) AS channel_group,
    SUM(new_subscriptions) OVER (
      PARTITION BY
        subscription_start_date
    ) AS total_new_subscriptions_for_date,
  FROM
    stage_1
)
SELECT
  *,
  SUM(new_subscriptions) OVER (
    PARTITION BY
      subscription_start_date,
      channel_group
  ) / total_new_subscriptions_for_date * 100 AS channel_group_percent_of_total_for_date,
FROM
  stage_2
