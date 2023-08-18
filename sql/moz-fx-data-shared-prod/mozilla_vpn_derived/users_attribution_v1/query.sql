SELECT
  id AS user_id,
  STRUCT(
    JSON_VALUE(attribution, '$.referrer') AS referrer,
    JSON_VALUE(attribution, '$.entrypoint_experiment') AS entrypoint_experiment,
    JSON_VALUE(attribution, '$.entrypoint_variation') AS entrypoint_variation,
    JSON_VALUE(attribution, '$.utm_campaign') AS utm_campaign,
    JSON_VALUE(attribution, '$.utm_content') AS utm_content,
    JSON_VALUE(attribution, '$.utm_medium') AS utm_medium,
    JSON_VALUE(attribution, '$.utm_source') AS utm_source,
    JSON_VALUE(attribution, '$.utm_term') AS utm_term,
    JSON_VALUE(attribution, '$.data_cta_position') AS data_cta_position
  ) AS attribution
FROM
  `moz-fx-data-shared-prod.mozilla_vpn_external.users_attribution_v1`
WHERE
  attribution IS NOT NULL
  AND attribution != '{}'
