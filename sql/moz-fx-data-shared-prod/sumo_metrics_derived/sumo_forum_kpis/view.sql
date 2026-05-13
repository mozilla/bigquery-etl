CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_forum_kpis`
AS
SELECT
  DATE,
  product,
  locale,
  questions_created,
  questions_replied,
  SAFE_DIVIDE(questions_replied, questions_created) AS reply_rate,
  avg_ttfr_hrs,
  total_ttfr_hrs,
  total_helpful_votes,
  total_unhelpful_votes,
  SAFE_DIVIDE(total_helpful_votes, total_helpful_votes + total_unhelpful_votes) AS helpfulness_rate
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.kitsune_forum_metrics_daily_v1`
