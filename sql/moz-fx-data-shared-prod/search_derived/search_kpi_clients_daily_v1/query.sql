-- One row per desktop client per day: SAP searches and Suggest clicks, the search KPI's
-- components. SAP comes from search_clients_daily_glean_v1; Suggest clicks come from
-- urlbar_clients_daily_v1, mapped to components by kpi_component_map_v1 as of that day.
-- Mobile rows are added once the mobile dimensions table exists.
WITH component_map AS (
  SELECT
    result_type,
    component,
    definition_version
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform = 'desktop'
    AND effective_from <= @submission_date
    AND (effective_to IS NULL OR effective_to > @submission_date)
),
-- Two rows in effect for one type would count its clicks twice.
component_map_check AS (
  SELECT
    IF(
      COUNT(*) > COUNT(DISTINCT result_type),
      ERROR('kpi_component_map_v1 has overlapping rows for a result type'),
      TRUE
    ) AS ok
  FROM
    component_map
),
-- Over every row started by this date, so closing a row can't lower the version.
definition AS (
  SELECT
    MAX(definition_version) AS definition_version
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform = 'desktop'
    AND effective_from <= @submission_date
),
urlbar AS (
  SELECT
    client_id,
    profile_group_id,
    sample_id,
    is_bot,
    results
  FROM
    `moz-fx-data-shared-prod.search_derived.urlbar_clients_daily_v1`
  WHERE
    submission_date = @submission_date
),
-- urlbar_clients_daily_v1 has no date_partition_offset, so the urlbar task this run waits on
-- writes the next day. The partition read here comes from the previous run. If that run
-- didn't land, fail rather than write a day with no Suggest clicks and no urlbar-only clients.
urlbar_partition_check AS (
  SELECT
    IF(
      COUNT(*) = 0,
      ERROR(FORMAT('urlbar_clients_daily_v1 has no rows for %t', @submission_date)),
      TRUE
    ) AS ok
  FROM
    urlbar
),
clicked_results AS (
  SELECT
    u.client_id,
    r.result_type,
    -- Quick actions are never results; new action_* types are excluded without a map row.
    COALESCE(m.component, IF(STARTS_WITH(r.result_type, 'action_'), 'excluded', NULL)) AS component,
    r.clicks
  FROM
    urlbar AS u
  CROSS JOIN
    UNNEST(u.results) AS r
  LEFT JOIN
    component_map AS m
    USING (result_type)
  WHERE
    r.clicks > 0
),
-- A clicked result type with no map row must fail the run, not fall out of the KPI.
unmapped_check AS (
  SELECT
    IF(
      COUNT(*) > 0,
      ERROR(
        CONCAT(
          'Result types missing from kpi_component_map_v1: ',
          STRING_AGG(DISTINCT result_type, ', ')
        )
      ),
      TRUE
    ) AS ok
  FROM
    clicked_results
  WHERE
    component IS NULL
),
suggest AS (
  SELECT
    client_id,
    SUM(IF(component = 'suggest_sponsored', clicks, 0)) AS suggest_sponsored,
    SUM(IF(component = 'suggest_nonsponsored', clicks, 0)) AS suggest_nonsponsored,
    SUM(IF(component = 'ai_chat', clicks, 0)) AS ai_chat
  FROM
    clicked_results
  GROUP BY
    client_id
),
urlbar_clients AS (
  SELECT
    u.client_id,
    u.profile_group_id,
    u.sample_id,
    u.is_bot,
    TRUE AS in_urlbar,
    COALESCE(s.suggest_sponsored, 0) AS suggest_sponsored,
    COALESCE(s.suggest_nonsponsored, 0) AS suggest_nonsponsored,
    COALESCE(s.ai_chat, 0) AS ai_chat
  FROM
    urlbar AS u
  LEFT JOIN
    suggest AS s
    USING (client_id)
),
sap AS (
  SELECT
    client_id,
    ANY_VALUE(sample_id) AS sample_id,
    SUM(IF(sap_exclusion_reason IS NULL, sap_counts_total, 0)) AS sap,
    SUM(IF(sap_exclusion_reason IS NOT NULL, sap_counts_total, 0)) AS sap_excluded
  FROM
    `moz-fx-data-shared-prod.search.search_clients_daily_glean`
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
),
dims AS (
  SELECT
    client_id,
    profile_group_id,
    sample_id,
    country,
    normalized_channel,
    os,
    app_version_major,
    normalized_default_search_engine,
    is_default_browser
  FROM
    `moz-fx-data-shared-prod.search_derived.desktop_client_day_dims_v1`
  WHERE
    submission_date = @submission_date
),
desktop AS (
  SELECT
    client_id,
    -- urlbar's profile_group_id first: baseline lacks it for some clients.
    COALESCE(u.profile_group_id, d.profile_group_id) AS profile_group_id,
    COALESCE(u.sample_id, s.sample_id, d.sample_id) AS sample_id,
    d.country,
    d.normalized_channel,
    d.os,
    d.app_version_major,
    d.normalized_default_search_engine,
    d.is_default_browser,
    COALESCE(s.sap, 0) AS sap,
    COALESCE(s.sap_excluded, 0) AS sap_excluded,
    COALESCE(u.suggest_sponsored, 0) AS suggest_sponsored,
    COALESCE(u.suggest_nonsponsored, 0) AS suggest_nonsponsored,
    COALESCE(u.ai_chat, 0) AS ai_chat,
    u.is_bot,
    COALESCE(u.in_urlbar, FALSE) AS in_urlbar
  FROM
    sap AS s
  FULL OUTER JOIN
    urlbar_clients AS u
    USING (client_id)
  LEFT JOIN
    dims AS d
    USING (client_id)
)
SELECT
  @submission_date AS submission_date,
  'desktop' AS platform,
  client_id,
  profile_group_id,
  sample_id,
  country,
  normalized_channel,
  os,
  app_version_major,
  normalized_default_search_engine,
  is_default_browser,
  sap,
  sap_excluded,
  suggest_sponsored,
  suggest_nonsponsored,
  ai_chat,
  sap + suggest_sponsored + suggest_nonsponsored + ai_chat AS search_queries,
  is_bot,
  in_urlbar,
  -- Firefox 138 is the SAP probe floor; it applies to Suggest too so both halves match.
  COALESCE(app_version_major >= 138, FALSE) AS meets_population_rules,
  definition.definition_version
FROM
  desktop
CROSS JOIN
  definition
CROSS JOIN
  unmapped_check
CROSS JOIN
  urlbar_partition_check
CROSS JOIN
  component_map_check
WHERE
  unmapped_check.ok
  AND urlbar_partition_check.ok
  AND component_map_check.ok
  -- Every urlbar user stays, even with nothing counted. A client seen only in
  -- search_clients_daily stays only if it started a search, counted or excluded.
  AND (in_urlbar OR sap > 0 OR sap_excluded > 0)
