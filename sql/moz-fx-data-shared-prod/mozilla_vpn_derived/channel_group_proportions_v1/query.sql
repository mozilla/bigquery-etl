WITH stage_1 AS (
  SELECT
    event_date AS subscription_start_date,
    country_name,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    entrypoint_experiment,
    entrypoint_variation,
    pricing_plan,
    provider,
    SUM(`count`) AS new_subscriptions,
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.subscription_events_v1
  WHERE
    event_type = "New"
    AND IF(@date IS NULL, event_date < CURRENT_DATE, event_date = @date)
  GROUP BY
    subscription_start_date,
    country_name,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    entrypoint_experiment,
    entrypoint_variation,
    pricing_plan,
    provider
),
stage_2 AS (
  SELECT
    *,
    mozfun.vpn.channel_group(utm_campaign, utm_content, utm_medium, utm_source) AS channel_group,
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
