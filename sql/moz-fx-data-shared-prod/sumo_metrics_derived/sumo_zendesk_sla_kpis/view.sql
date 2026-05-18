CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_zendesk_sla_kpis`
AS
-- SLA KPI components aggregated from zendesk_ticket_sla_v1.
--
-- Resolved-ticket metrics (tickets_resolved, one_touch_tickets, satisfied_surveys,
-- rated_surveys) are gated to resolution_status = 'Resolved' and attributed to
-- resolved_date (mirrors Zendesk Explore). Reply/duration metrics
-- (replied_tickets, first_reply_business_minutes_sum, full_resolution_business_minutes_sum)
-- include unresolved tickets so TTFR reflects every replied ticket; rows for
-- unresolved tickets have resolved_date IS NULL.
--
-- Exposes sum/count component columns rather than precomputed rates so the
-- KPIs remain correct under any downstream filter or grouping. Compute as:
--   FCR %        = SUM(one_touch_tickets)                    / NULLIF(SUM(tickets_resolved), 0) * 100
--   CSAT %       = SUM(satisfied_surveys)                    / NULLIF(SUM(rated_surveys),     0) * 100
--   Avg TTFR min = SUM(first_reply_business_minutes_sum)     / NULLIF(SUM(replied_tickets),   0)
--   Avg TRT min  = SUM(full_resolution_business_minutes_sum) / NULLIF(SUM(tickets_resolved), 0)
WITH base_tickets AS (
  SELECT
    DATE(full_resolution_at) AS resolved_date,
    ticket_created_date,
    product,
    automation_category,
    resolution_status,
    is_one_touch,
    rating_category,
    first_reply_time_business_minutes,
    full_resolution_time_business_minutes
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.zendesk_ticket_sla_v1`
  WHERE
    NOT is_excluded_from_sla
)
SELECT
  resolved_date,
  ticket_created_date,
  product,
  automation_category,
  COUNTIF(resolution_status = 'Resolved') AS tickets_resolved,
  COUNTIF(resolution_status = 'Resolved' AND is_one_touch = 1) AS one_touch_tickets,
  COUNTIF(resolution_status = 'Resolved' AND rating_category = 'good') AS satisfied_surveys,
  COUNTIF(resolution_status = 'Resolved' AND rating_category IN ('good', 'bad')) AS rated_surveys,
  -- TTFR denominator: not every ticket has a first agent reply (auto-solved
  -- tickets can resolve without one), so use a dedicated count.
  COUNTIF(first_reply_time_business_minutes IS NOT NULL) AS replied_tickets,
  SUM(first_reply_time_business_minutes) AS first_reply_business_minutes_sum,
  SUM(full_resolution_time_business_minutes) AS full_resolution_business_minutes_sum
FROM
  base_tickets
GROUP BY
  resolved_date,
  ticket_created_date,
  product,
  automation_category
