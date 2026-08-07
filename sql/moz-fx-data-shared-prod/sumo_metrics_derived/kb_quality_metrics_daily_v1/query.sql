-- Day-level KB quality metrics: daily freshness/helpfulness totals plus their
-- 14-day moving averages and 14-day-lagged comparisons. Derived from the
-- article-grain table so the population and product filters stay in one place.
WITH day_level AS (
  SELECT
    event_date,
    SUM(fresh_ind) AS daily_fresh_count,
    COUNT(document_id) AS daily_article_count,
    SUM(helpful_votes) AS daily_helpful_votes,
    SUM(total_votes) AS daily_total_votes
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.kb_quality_metrics_article_v1`
  GROUP BY
    event_date
),
moving_averages AS (
  SELECT
    *,
    AVG(daily_fresh_count) OVER (
      ORDER BY
        event_date
      ROWS BETWEEN
        13 PRECEDING
        AND CURRENT ROW
    ) AS fresh_count_14_ma,
    AVG(daily_article_count) OVER (
      ORDER BY
        event_date
      ROWS BETWEEN
        13 PRECEDING
        AND CURRENT ROW
    ) AS article_count_14_ma,
    AVG(daily_helpful_votes) OVER (
      ORDER BY
        event_date
      ROWS BETWEEN
        13 PRECEDING
        AND CURRENT ROW
    ) AS helpful_votes_14_ma,
    AVG(daily_total_votes) OVER (
      ORDER BY
        event_date
      ROWS BETWEEN
        13 PRECEDING
        AND CURRENT ROW
    ) AS total_votes_14_ma
  FROM
    day_level
)
SELECT
  *,
  LAG(fresh_count_14_ma, 14) OVER (ORDER BY event_date) AS lagged_fresh_count_14_ma,
  LAG(article_count_14_ma, 14) OVER (ORDER BY event_date) AS lagged_article_count_14_ma,
  LAG(helpful_votes_14_ma, 14) OVER (ORDER BY event_date) AS lagged_helpful_votes_14_ma,
  LAG(total_votes_14_ma, 14) OVER (ORDER BY event_date) AS lagged_total_votes_14_ma,
  SAFE_DIVIDE(fresh_count_14_ma, article_count_14_ma) AS freshness_14_ma,
  SAFE_DIVIDE(helpful_votes_14_ma, total_votes_14_ma) AS helpful_percentage_14_ma,
  SAFE_DIVIDE(
    LAG(fresh_count_14_ma, 14) OVER (ORDER BY event_date),
    LAG(article_count_14_ma, 14) OVER (ORDER BY event_date)
  ) AS lagged_freshness_14_ma,
  SAFE_DIVIDE(
    LAG(helpful_votes_14_ma, 14) OVER (ORDER BY event_date),
    LAG(total_votes_14_ma, 14) OVER (ORDER BY event_date)
  ) AS lagged_helpful_percentage_14_ma
FROM
  moving_averages
