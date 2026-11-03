#fail
{{ is_unique(["platform", "client_id"], "submission_date = @submission_date") }}

#fail
-- Every SAP search in the source must land here: the only clients left out have sap = 0.
WITH kpi AS (
  SELECT
    SUM(sap) AS sap
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND platform = 'desktop'
),
source AS (
  SELECT
    SUM(sap_counts_total) AS sap
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    kpi.sap IS DISTINCT FROM source.sap,
    ERROR(
      FORMAT("Desktop sap is %d here but %d in search_clients_daily_glean_v1", kpi.sap, source.sap)
    ),
    NULL
  )
FROM
  kpi,
  source;

#fail
-- Every Suggest and AI chat click in urlbar_clients_daily_v1 must land here: every urlbar
-- client is kept, so the totals match exactly.
WITH kpi AS (
  SELECT
    SUM(suggest_sponsored) AS suggest_sponsored,
    SUM(suggest_nonsponsored) AS suggest_nonsponsored,
    SUM(ai_chat) AS ai_chat
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND platform = 'desktop'
),
component_map AS (
  SELECT
    result_type,
    component
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform = 'desktop'
    AND effective_from <= @submission_date
    AND (effective_to IS NULL OR effective_to > @submission_date)
),
source AS (
  SELECT
    COALESCE(SUM(IF(m.component = 'suggest_sponsored', r.clicks, 0)), 0) AS suggest_sponsored,
    COALESCE(SUM(IF(m.component = 'suggest_nonsponsored', r.clicks, 0)), 0) AS suggest_nonsponsored,
    COALESCE(SUM(IF(m.component = 'ai_chat', r.clicks, 0)), 0) AS ai_chat
  FROM
    `moz-fx-data-shared-prod.search_derived.urlbar_clients_daily_v1` AS u
  CROSS JOIN
    UNNEST(u.results) AS r
  JOIN
    component_map AS m
    USING (result_type)
  WHERE
    u.submission_date = @submission_date
)
SELECT
  IF(
    kpi.suggest_sponsored IS DISTINCT FROM source.suggest_sponsored
    OR kpi.suggest_nonsponsored IS DISTINCT FROM source.suggest_nonsponsored
    OR kpi.ai_chat IS DISTINCT FROM source.ai_chat,
    ERROR(
      FORMAT(
        "Desktop Suggest clicks (sponsored, nonsponsored, ai_chat) are (%d, %d, %d) here but (%d, %d, %d) in urlbar_clients_daily_v1",
        kpi.suggest_sponsored,
        kpi.suggest_nonsponsored,
        kpi.ai_chat,
        source.suggest_sponsored,
        source.suggest_nonsponsored,
        source.ai_chat
      )
    ),
    NULL
  )
FROM
  kpi,
  source;
