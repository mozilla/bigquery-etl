WITH services AS (
  SELECT
    *
  FROM
    UNNEST(
      [
        STRUCT(
          'VPN' AS id,
          'Mozilla VPN' AS name,
          ARRAY<STRUCT<name STRING, subplat_capabilities ARRAY<STRING>>>[] AS tiers,
          ['guardian_vpn_1'] AS subplat_capabilities,
          [STRUCT('e6eb0d1e856335fc' AS id, 'guardian-vpn' AS name)] AS subplat_oauth_clients
        ),
        STRUCT(
          'FPN' AS id,
          'Firefox Private Network' AS name,
          ARRAY<STRUCT<name STRING, subplat_capabilities ARRAY<STRING>>>[] AS tiers,
          ['fpn-browser'] AS subplat_capabilities,
          [STRUCT('565585c1745a144d' AS id, 'fx-priv-network' AS name)] AS subplat_oauth_clients
        ),
        STRUCT(
          'Relay' AS id,
          'Relay Premium' AS name,
          [
            STRUCT('Email & Phone Masking' AS name, ['relay-phones'] AS subplat_capabilities),
            STRUCT('Email Masking' AS name, ['premium-relay'] AS subplat_capabilities)
          ] AS tiers,
          ['relay-phones', 'premium-relay'] AS subplat_capabilities,
          [STRUCT('9ebfe2c2f9ea3c58' AS id, 'fx-private-relay' AS name)] AS subplat_oauth_clients
        ),
        STRUCT(
          'MDN' AS id,
          'MDN Plus' AS name,
          [
            STRUCT(
              'Supporter 10' AS name,
              ['mdn_plus_10m', 'mdn_plus_10y'] AS subplat_capabilities
            ),
            STRUCT('Plus 5' AS name, ['mdn_plus_5m', 'mdn_plus_5y'] AS subplat_capabilities)
          ] AS tiers,
          [
            'mdn_plus',
            'mdn_plus_10m',
            'mdn_plus_10y',
            'mdn_plus_5m',
            'mdn_plus_5y'
          ] AS subplat_capabilities,
          [STRUCT('720bc80adfa6988d' AS id, 'mdn-plus' AS name)] AS subplat_oauth_clients
        ),
        STRUCT(
          'Hubs' AS id,
          'Hubs' AS name,
          [
            STRUCT('Professional' AS name, ['hubs-professional'] AS subplat_capabilities),
            STRUCT('Personal' AS name, ['managed-hubs'] AS subplat_capabilities)
          ] AS tiers,
          ['hubs-professional', 'managed-hubs'] AS subplat_capabilities,
          [
            STRUCT('8c3e3e6de4ee9731' AS id, 'mozilla-hubs' AS name),
            STRUCT('34bc0d0a6add7329' AS id, 'mozilla-hubs-dev' AS name)
          ] AS subplat_oauth_clients
        )
      ]
    )
),
stripe_products AS (
  SELECT
    id,
    PARSE_JSON(metadata) AS metadata
  FROM
    `moz-fx-data-shared-prod.stripe_external.product_v1`
),
stripe_plans AS (
  SELECT
    id,
    PARSE_JSON(metadata) AS metadata,
    product_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.plan_v1`
),
service_stripe_product_ids AS (
  SELECT
    services.id AS service_id,
    ARRAY_AGG(DISTINCT stripe_products.id ORDER BY stripe_products.id) AS stripe_product_ids
  FROM
    services
  CROSS JOIN
    UNNEST(services.subplat_oauth_clients) AS subplat_oauth_client
  CROSS JOIN
    stripe_products
  JOIN
    UNNEST(
      SPLIT(JSON_VALUE(stripe_products.metadata['capabilities:' || subplat_oauth_client.id]), ',')
    ) AS capability
  ON
    TRIM(capability) IN UNNEST(services.subplat_capabilities)
  GROUP BY
    service_id
),
service_stripe_plan_capabilities AS (
  SELECT DISTINCT
    services.id AS service_id,
    stripe_plans.id AS stripe_plan_id,
    TRIM(capability) AS capability
  FROM
    services
  CROSS JOIN
    UNNEST(services.subplat_oauth_clients) AS subplat_oauth_client
  CROSS JOIN
    stripe_plans
  LEFT JOIN
    stripe_products
  ON
    stripe_plans.product_id = stripe_products.id
  JOIN
    UNNEST(
      SPLIT(
        COALESCE(
          JSON_VALUE(stripe_plans.metadata['capabilities:' || subplat_oauth_client.id]),
          JSON_VALUE(stripe_products.metadata['capabilities:' || subplat_oauth_client.id])
        ),
        ','
      )
    ) AS capability
  ON
    TRIM(capability) IN UNNEST(services.subplat_capabilities)
),
service_stripe_plan_ids AS (
  SELECT
    service_id,
    ARRAY_AGG(DISTINCT stripe_plan_id ORDER BY stripe_plan_id) AS stripe_plan_ids
  FROM
    service_stripe_plan_capabilities
  GROUP BY
    service_id
),
service_stripe_plan_tier_names AS (
  SELECT
    service_stripe_plan_capabilities.service_id,
    service_stripe_plan_capabilities.stripe_plan_id,
    -- Pick the first tier that matches (tiers should be in order of precedence).
    ARRAY_AGG(tier.name ORDER BY tier_order LIMIT 1)[ORDINAL(1)] tier_name
  FROM
    service_stripe_plan_capabilities
  JOIN
    services
  ON
    service_stripe_plan_capabilities.service_id = services.id
  JOIN
    UNNEST(services.tiers) AS tier
    WITH OFFSET AS tier_order
  ON
    service_stripe_plan_capabilities.capability IN UNNEST(tier.subplat_capabilities)
  GROUP BY
    service_id,
    stripe_plan_id
),
service_tier_stripe_plan_ids AS (
  SELECT
    service_id,
    tier_name,
    ARRAY_AGG(DISTINCT stripe_plan_id ORDER BY stripe_plan_id) AS stripe_plan_ids
  FROM
    service_stripe_plan_tier_names
  GROUP BY
    service_id,
    tier_name
),
service_tiers AS (
  SELECT
    services.id AS service_id,
    ARRAY_AGG(
      STRUCT(tier.name, tier.subplat_capabilities, service_tier_stripe_plan_ids.stripe_plan_ids)
      ORDER BY
        tier_order
    ) AS tiers
  FROM
    services
  CROSS JOIN
    UNNEST(services.tiers) AS tier
    WITH OFFSET AS tier_order
  LEFT JOIN
    service_tier_stripe_plan_ids
  ON
    services.id = service_tier_stripe_plan_ids.service_id
    AND tier.name = service_tier_stripe_plan_ids.tier_name
  GROUP BY
    service_id
)
SELECT
  services.* REPLACE (service_tiers.tiers AS tiers),
  service_stripe_product_ids.stripe_product_ids,
  service_stripe_plan_ids.stripe_plan_ids
FROM
  services
LEFT JOIN
  service_tiers
ON
  services.id = service_tiers.service_id
LEFT JOIN
  service_stripe_product_ids
ON
  services.id = service_stripe_product_ids.service_id
LEFT JOIN
  service_stripe_plan_ids
ON
  services.id = service_stripe_plan_ids.service_id
