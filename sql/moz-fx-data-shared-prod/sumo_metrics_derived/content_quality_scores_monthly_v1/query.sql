-- Article x month content quality SCORES. Each article-month is scored on a
-- 0-1 percentile basis against a fixed 2024-H2 baseline population, then those
-- components are combined into a weighted 0-100 quality_score. Built from
-- content_quality_metrics_article_monthly_v1 so the metric definitions stay
-- upstream.
--
-- Percentiles are computed once over the baseline (one row per article), and
-- each article-month is positioned relative to that baseline distribution:
--   freshness / engagement -> higher is better
--   helpfulness            -> higher is better (NULL = neutral 0.5)
--   bounce / exit          -> lower is better
--
-- Only article-months with >= 100 page views are scored. Full recompute each
-- run (no date partition).
WITH article_monthly AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.content_quality_metrics_article_monthly_v1`
),
-- 2024-H2 baseline: one row per article (locale, slug), averaged over Jul-Dec
-- 2024 months that individually cleared the 100-page-view bar.
baseline AS (
  SELECT
    locale,
    slug,
    AVG(freshness_months) AS freshness_months,
    AVG(helpfulness) AS helpfulness,
    AVG(avg_engagement_time) AS avg_engagement_time,
    AVG(bounce_pageview_share) AS bounce_pageview_share,
    AVG(exit_rate) AS exit_rate,
    SUM(page_views) AS total_page_views
  FROM
    article_monthly
  WHERE
    MONTH
    BETWEEN '2024-07-01'
    AND '2024-12-01'
    AND page_views >= 100
  GROUP BY
    locale,
    slug
  HAVING
    SUM(page_views) >= 100
),
-- Percentile rank of each baseline article within the baseline distribution.
baseline_percentiles AS (
  SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY freshness_months DESC) AS freshness_percentile_in_baseline,
    PERCENT_RANK() OVER (
      ORDER BY
        COALESCE(helpfulness, 0.5) ASC
    ) AS helpfulness_percentile_in_baseline,
    PERCENT_RANK() OVER (ORDER BY avg_engagement_time ASC) AS engagement_percentile_in_baseline,
    PERCENT_RANK() OVER (ORDER BY bounce_pageview_share DESC) AS bounce_percentile_in_baseline,
    PERCENT_RANK() OVER (ORDER BY exit_rate ASC) AS exit_percentile_in_baseline
  FROM
    baseline
),
-- Position each article-month against the baseline distribution.
scored AS (
  SELECT
    ams.month,
    ams.locale,
    ams.slug,
    ams.title,
    ams.product,
    ams.freshness_months,
    ams.helpfulness,
    ams.avg_engagement_time,
    ams.landing_rate,
    ams.exit_rate,
    ams.bounce_pageview_share,
    ams.page_views,
    COALESCE(
      (
        SELECT
          MAX(freshness_percentile_in_baseline)
        FROM
          baseline_percentiles b
        WHERE
          b.freshness_months >= ams.freshness_months
      ),
      0.0
    ) AS freshness_percentile,
    CASE
      WHEN ams.helpfulness IS NULL
        THEN 0.5 -- no votes = neutral
      ELSE COALESCE(
          (
            SELECT
              MAX(helpfulness_percentile_in_baseline)
            FROM
              baseline_percentiles b
            WHERE
              b.helpfulness <= ams.helpfulness
          ),
          0.0
        )
    END AS helpfulness_percentile,
    COALESCE(
      (
        SELECT
          MAX(engagement_percentile_in_baseline)
        FROM
          baseline_percentiles b
        WHERE
          b.avg_engagement_time <= ams.avg_engagement_time
      ),
      0.0
    ) AS engagement_percentile,
    COALESCE(
      (
        SELECT
          MAX(bounce_percentile_in_baseline)
        FROM
          baseline_percentiles b
        WHERE
          b.bounce_pageview_share >= ams.bounce_pageview_share
      ),
      0.0
    ) AS bounce_percentile,
    COALESCE(
      (
        SELECT
          MAX(exit_percentile_in_baseline)
        FROM
          baseline_percentiles b
        WHERE
          b.exit_rate <= ams.exit_rate
      ),
      0.0
    ) AS exit_percentile
  FROM
    article_monthly ams
  WHERE
    ams.page_views >= 100
)
SELECT
  *,
  ROUND(
    freshness_percentile * 25 -- 25% weight
    + helpfulness_percentile * 35 -- 35% weight
    + engagement_percentile * 20 -- 20% weight
    + bounce_percentile * 10 -- 10% weight
    + exit_percentile * 10, -- 10% weight
    2
  ) AS quality_score
FROM
  scored
