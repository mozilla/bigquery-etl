CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_content_quality_kpis`
AS
-- Monthly KB content quality KPIs for SUMO, by product and overall ('ALL'),
-- built from content_quality_scores_monthly_v1. Composite and component scores
-- are exposed both page-view-weighted and as simple (unweighted) averages, plus
-- a median composite. Per-product and all-product rows are produced together via
-- GROUPING SETS; the all-product row has a NULL product, surfaced as 'ALL'.
SELECT
  `month`,
  IFNULL(product, 'ALL') AS product,
  COUNT(*) AS article_count,
  SUM(page_views) AS total_page_views,
  ROUND(
    SAFE_DIVIDE(SUM(quality_score * page_views), SUM(page_views)),
    2
  ) AS avg_quality_score_weighted,
  ROUND(AVG(quality_score), 2) AS avg_quality_score_simple,
  ROUND(APPROX_QUANTILES(quality_score, 2)[OFFSET(1)], 2) AS median_quality_score,
  ROUND(
    SAFE_DIVIDE(SUM(freshness_percentile * page_views), SUM(page_views)) * 100,
    1
  ) AS avg_freshness_score_weighted,
  ROUND(
    SAFE_DIVIDE(SUM(helpfulness_percentile * page_views), SUM(page_views)) * 100,
    1
  ) AS avg_helpfulness_score_weighted,
  ROUND(
    SAFE_DIVIDE(SUM(engagement_percentile * page_views), SUM(page_views)) * 100,
    1
  ) AS avg_engagement_score_weighted,
  ROUND(
    SAFE_DIVIDE(SUM(bounce_percentile * page_views), SUM(page_views)) * 100,
    1
  ) AS avg_bounce_score_weighted,
  ROUND(
    SAFE_DIVIDE(SUM(exit_percentile * page_views), SUM(page_views)) * 100,
    1
  ) AS avg_exit_score_weighted,
  ROUND(AVG(freshness_percentile) * 100, 2) AS avg_freshness_score_simple,
  ROUND(AVG(helpfulness_percentile) * 100, 2) AS avg_helpfulness_score_simple,
  ROUND(AVG(engagement_percentile) * 100, 2) AS avg_engagement_score_simple,
  ROUND(AVG(bounce_percentile) * 100, 2) AS avg_bounce_score_simple,
  ROUND(AVG(exit_percentile) * 100, 2) AS avg_exit_score_simple
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.content_quality_scores_monthly_v1`
GROUP BY
  GROUPING SETS ((`month`, product), (`month`))
