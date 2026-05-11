CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_zendesk_sla_kpis`
AS
-- CSAT and FCR KPIs aggregated from zendesk_ticket_sla_v1.
-- Both metrics are attributed to the date the ticket was resolved, matching
-- how Zendesk Explore reports "% One-touch tickets" and CSAT.
WITH resolved_tickets AS (
  SELECT
    DATE(full_resolution_at) AS resolved_date,
    product,
    automation_category,
    is_one_touch,
    rating_category
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
  -- FCR (%) = one-touch resolved tickets / total resolved tickets * 100.
  -- Mirrors Zendesk Explore's "% One-touch tickets" metric.
  SAFE_DIVIDE(COUNTIF(is_one_touch = 1), COUNT(*)) * 100 AS fcr_percentage,
  -- CSAT (%) = good ratings / (good + bad ratings) * 100.
  -- Tickets without a rating are excluded from the denominator.
  SAFE_DIVIDE(
    COUNTIF(rating_category = 'good'),
    COUNTIF(rating_category IN ('good', 'bad'))
  ) * 100 AS csat_percentage
FROM
  resolved_tickets
GROUP BY
  resolved_date,
  product,
  automation_category
