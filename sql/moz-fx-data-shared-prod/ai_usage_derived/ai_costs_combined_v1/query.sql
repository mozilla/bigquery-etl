SELECT
  `date`,
  'OpenAI' AS provider,
  amount_value AS amount,
  currency,
  project_id,
  organization_id,
  line_item AS cost_description,
  CAST(NULL AS STRING) AS workspace_id,
  CAST(NULL AS STRING) AS model,
  CAST(NULL AS STRING) AS token_type,
  CAST(NULL AS STRING) AS cost_type,
  CAST(NULL AS STRING) AS context_window,
  CAST(NULL AS STRING) AS service_tier,
  CAST(NULL AS STRING) AS inference_geo
FROM
  `moz-fx-data-shared-prod.ai_usage_derived.openai_costs_v1`
UNION ALL
SELECT
  `date`,
  'Claude' AS provider,
  amount / 100.0 AS amount,
  currency,
  CAST(NULL AS STRING) AS project_id,
  CAST(NULL AS STRING) AS organization_id,
  description AS cost_description,
  workspace_id,
  model,
  token_type,
  cost_type,
  context_window,
  service_tier,
  inference_geo
FROM
  `moz-fx-data-shared-prod.ai_usage_derived.claude_costs_v1`
