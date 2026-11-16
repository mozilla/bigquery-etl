CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.search_kpi_daily`
AS
SELECT
  submission_date,
  platform,
  IF(platform = 'desktop', 'desktop', 'mobile') AS device,
  country,
  normalized_channel,
  os,
  app_version_major,
  normalized_default_search_engine,
  definition_version,
  component,
  CASE
    component
    WHEN 'sap'
      THEN 'SAP searches'
    WHEN 'suggest_sponsored'
      THEN 'Sponsored Suggest clicks'
    WHEN 'suggest_nonsponsored'
      THEN 'Non-sponsored Suggest clicks'
    WHEN 'ai_chat'
      THEN 'AI chat clicks'
    ELSE component
  END AS component_label,
  value,
  client_count_by_kpi_component
FROM
  `moz-fx-data-shared-prod.search_derived.search_kpi_daily_v1`
WHERE
  meets_population_rules
  AND is_bot IS NOT TRUE
  AND component != 'sap_excluded'
