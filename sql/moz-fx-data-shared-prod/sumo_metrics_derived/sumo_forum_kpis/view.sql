CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_forum_kpis`
AS
SELECT
  created_date,
  first_reply_date,
  product,
  locale,
  questions_created,
  questions_replied,
  avg_ttfr_hrs,
  total_ttfr_hrs,
  total_helpful_votes,
  total_unhelpful_votes
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.kitsune_forum_metrics_daily_v1`
