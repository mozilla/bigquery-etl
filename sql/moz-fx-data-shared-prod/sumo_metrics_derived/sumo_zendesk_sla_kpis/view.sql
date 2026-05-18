CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_zendesk_sla_kpis`
AS
-- SLA KPI components aggregated from zendesk_ticket_sla_v1, attributed to the
-- date the ticket was resolved (mirrors Zendesk Explore).
--
-- Exposes sum/count component columns rather than precomputed rates so the
-- KPIs remain correct under any downstream filter or grouping. Compute as:
--   FCR %        = SUM(one_touch_tickets)                    / NULLIF(SUM(tickets_resolved), 0) * 100
--   CSAT %       = SUM(satisfied_surveys)                    / NULLIF(SUM(rated_surveys),     0) * 100
--   Avg TTFR min = SUM(first_reply_business_minutes_sum)     / NULLIF(SUM(replied_tickets),   0)
--   Avg TRT min  = SUM(full_resolution_business_minutes_sum) / NULLIF(SUM(tickets_resolved), 0)
WITH resolved_tickets AS (
  SELECT
    DATE(full_resolution_at) AS resolved_date,
    product,
    automation_category,
    is_one_touch,
    rating_category,
    first_reply_time_business_minutes,
    full_resolution_time_business_minutes
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.zendesk_ticket_sla_v1`
  WHERE
    NOT is_excluded_from_sla
    AND resolution_status = 'Resolved'
)
SELECT
  resolved_date AS `date`,
  product,
  automation_category,
  COUNT(*) AS tickets_resolved,
  COUNTIF(is_one_touch = 1) AS one_touch_tickets,
  COUNTIF(rating_category = 'good') AS satisfied_surveys,
  COUNTIF(rating_category IN ('good', 'bad')) AS rated_surveys,
  -- TTFR denominator: not every resolved ticket has a first agent reply
  -- (auto-solved tickets can resolve without one), so use a dedicated count.
  COUNTIF(first_reply_time_business_minutes IS NOT NULL) AS replied_tickets,
  SUM(first_reply_time_business_minutes) AS first_reply_business_minutes_sum,
  SUM(full_resolution_time_business_minutes) AS full_resolution_business_minutes_sum
FROM
  resolved_tickets
GROUP BY
  resolved_date,
  product,
  automation_category
