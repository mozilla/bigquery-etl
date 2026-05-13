CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_zendesk_kpis`
AS
SELECT
  DATE,
  product,
  SUM(tickets_created) AS tickets_created,
  SUM(tickets_resolved) AS tickets_resolved,
  SAFE_DIVIDE(
    SUM(CASE WHEN automation_category = 'automation' THEN tickets_resolved END),
    SUM(tickets_resolved)
  ) AS automation_resolution_rate
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.zendesk_automation_daily_v1`
GROUP BY
  DATE,
  product
