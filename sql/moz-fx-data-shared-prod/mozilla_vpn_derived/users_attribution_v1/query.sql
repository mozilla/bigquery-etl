WITH users_attribution AS (
  SELECT
    id AS user_id,
    STRUCT(
      NULLIF(JSON_VALUE(attribution, '$.referrer'), '') AS referrer,
      JSON_VALUE(attribution, '$.entrypoint_experiment') AS entrypoint_experiment,
      JSON_VALUE(attribution, '$.entrypoint_variation') AS entrypoint_variation,
      JSON_VALUE(attribution, '$.utm_campaign') AS utm_campaign,
      JSON_VALUE(attribution, '$.utm_content') AS utm_content,
      NULLIF(NULLIF(JSON_VALUE(attribution, '$.utm_medium'), ''), '(not set)') AS utm_medium,
      NULLIF(NULLIF(JSON_VALUE(attribution, '$.utm_source'), ''), '(not set)') AS utm_source,
      JSON_VALUE(attribution, '$.utm_term') AS utm_term,
      JSON_VALUE(attribution, '$.data_cta_position') AS data_cta_position
    ) AS attribution
  FROM
    `moz-fx-data-shared-prod.mozilla_vpn_external.users_attribution_v1`
)
SELECT
  *
FROM
  users_attribution
WHERE
  attribution.referrer IS NOT NULL
  OR attribution.entrypoint_experiment IS NOT NULL
  OR attribution.entrypoint_variation IS NOT NULL
  OR attribution.utm_campaign IS NOT NULL
  OR attribution.utm_content IS NOT NULL
  OR attribution.utm_medium IS NOT NULL
  OR attribution.utm_source IS NOT NULL
  OR attribution.utm_term IS NOT NULL
  OR attribution.data_cta_position IS NOT NULL
