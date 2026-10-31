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
