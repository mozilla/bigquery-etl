WITH subscription_starts AS (
  SELECT
    subscription.id AS subscription_id,
    subscription.started_at,
    provider_subscriptions_history_id AS stripe_subscriptions_history_id
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_logical_subscriptions_history_v1`
  WHERE
    {% if is_init() %}
      DATE(subscription.started_at) <= DATE(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
    {% else %}
      DATE(subscription.started_at) = @date
    {% endif %}
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY subscription.id ORDER BY valid_from, valid_to)
)
SELECT
  subscription_starts.subscription_id,
  subscription_starts.started_at AS subscription_started_at,
  STRUCT(
    subscription_starts.started_at AS impression_at,
    history.subscription.metadata.session_entrypoint AS entrypoint,
    history.subscription.metadata.session_entrypoint_experiment AS entrypoint_experiment,
    history.subscription.metadata.session_entrypoint_variation AS entrypoint_variation,
    history.subscription.metadata.utm_campaign,
    history.subscription.metadata.utm_content,
    history.subscription.metadata.utm_medium,
    history.subscription.metadata.utm_source,
    history.subscription.metadata.utm_term
  ) AS last_touch_attribution
FROM
  subscription_starts
JOIN
  `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_history_v2` AS history
  ON subscription_starts.stripe_subscriptions_history_id = history.id
WHERE
  history.subscription.metadata.session_flow_id IS NOT NULL
  OR history.subscription.metadata.session_entrypoint IS NOT NULL
  OR history.subscription.metadata.session_entrypoint_experiment IS NOT NULL
  OR history.subscription.metadata.session_entrypoint_variation IS NOT NULL
  OR history.subscription.metadata.utm_campaign IS NOT NULL
  OR history.subscription.metadata.utm_content IS NOT NULL
  OR history.subscription.metadata.utm_medium IS NOT NULL
  OR history.subscription.metadata.utm_source IS NOT NULL
  OR history.subscription.metadata.utm_term IS NOT NULL
