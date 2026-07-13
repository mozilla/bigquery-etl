CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_content_quality_kpis`
AS
-- Day-level SUMO content quality score: a weighted blend of KB freshness,
-- KB helpfulness, support deflection (1 - SSER), and KB search success (SSR).
--
--   content_quality_score = freshness * 0.3
--                         + helpfulness * 0.4
--                         + deflection * 0.2
--                         + ssr * 0.1
--
-- Each component is sourced from its own KPI layer so the population, product,
-- and locale definitions stay in one place:
--   - freshness / helpfulness : kb_quality_metrics_daily_v1   (en-US articles)
--   - deflection              : sumo_sser_kpis                (all products/locales)
--   - search success (SSR)    : sumo_search_success_kpis      (en-US search locale)
--
-- The SSER and SSR 14-day moving averages are computed here at the day-level
-- grain (the upstream SSER view is per-product and the upstream SSR view's MA is
-- all-locale), so the smoothed score matches the original dashboard query.
WITH freshness AS (
  SELECT
    event_date,
    SAFE_DIVIDE(daily_fresh_count, daily_article_count) AS freshness,
    freshness_14_ma,
    lagged_freshness_14_ma,
    SAFE_DIVIDE(daily_helpful_votes, daily_total_votes) AS helpfulness,
    helpful_percentage_14_ma,
    lagged_helpful_percentage_14_ma
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.kb_quality_metrics_daily_v1`
),
-- Collapse the per-product SSER table to a single day-level self-service
-- escalation rate (all products, all locales), then smooth it.
daily_sser AS (
  SELECT
    event_date,
    SAFE_DIVIDE(
      SUM(zendesk_tickets + forum_questions),
      SUM(kb_sessions + forum_question_sessions)
    ) AS daily_sser
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_sser_kpis`
  GROUP BY
    event_date
),
deflection AS (
  SELECT
    event_date,
    1 - daily_sser AS daily_deflection,
    1 - sser_14_ma AS deflection_14_ma,
    1 - LAG(sser_14_ma, 14) OVER (ORDER BY event_date) AS lagged_deflection_14_ma
  FROM
    (
      SELECT
        ds.*,
        AVG(ds.daily_sser) OVER (
          ORDER BY
            ds.event_date
          ROWS BETWEEN
            13 PRECEDING
            AND CURRENT ROW
        ) AS sser_14_ma
      FROM
        daily_sser AS ds
    )
),
-- en-US daily search success rate, with a day-level 14-day moving average/lag.
ssr AS (
  SELECT
    session_date,
    ssr AS total_ssr,
    total_ssr_14_ma,
    LAG(total_ssr_14_ma, 14) OVER (ORDER BY session_date) AS lagged_total_ssr_14_ma
  FROM
    (
      SELECT
        session_date,
        ssr,
        AVG(ssr) OVER (
          ORDER BY
            session_date
          ROWS BETWEEN
            13 PRECEDING
            AND CURRENT ROW
        ) AS total_ssr_14_ma
      FROM
        `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_search_success_kpis`
      WHERE
        search_locale = 'en-US'
    )
)
SELECT
  f.event_date,
  f.freshness,
  f.freshness_14_ma,
  f.lagged_freshness_14_ma,
  f.helpfulness,
  f.helpful_percentage_14_ma,
  f.lagged_helpful_percentage_14_ma,
  d.daily_deflection,
  d.deflection_14_ma,
  d.lagged_deflection_14_ma,
  s.total_ssr,
  s.total_ssr_14_ma,
  s.lagged_total_ssr_14_ma,
  (f.freshness * 0.3) + (f.helpfulness * 0.4) + (d.daily_deflection * 0.2) + (
    s.total_ssr * 0.1
  ) AS content_quality_score,
  (f.freshness_14_ma * 0.3) + (f.helpful_percentage_14_ma * 0.4) + (d.deflection_14_ma * 0.2) + (
    s.total_ssr_14_ma * 0.1
  ) AS content_quality_score_14_ma
FROM
  freshness f
LEFT JOIN
  deflection d
  ON f.event_date = d.event_date
LEFT JOIN
  ssr s
  ON f.event_date = s.session_date
