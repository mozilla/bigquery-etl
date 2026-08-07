CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_sser_kpis`
AS
-- Pivot GA4 sessions into kb and forum columns so we can join once per date/product.
WITH ga4_pivoted AS (
  SELECT
    event_date,
    product,
    SUM(IF(content_type = 'kb', sessions, 0)) AS kb_sessions,
    SUM(IF(content_type = 'forum_question', sessions, 0)) AS forum_question_sessions
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.ga4_engagement_sessions_daily_v1`
  GROUP BY
    event_date,
    product
),
-- Union of all date/product keys across the three channels to form a complete spine.
spine AS (
  SELECT
    DATE AS event_date,
    product
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.zendesk_tickets_base_v1`
  UNION DISTINCT
  SELECT
    creation_date,
    product
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.kitsune_questions_base_v1`
  UNION DISTINCT
  SELECT
    event_date,
    product
  FROM
    ga4_pivoted
)
SELECT
  s.event_date AS event_date,
  s.product,
  -- Raw channel volumes (numerators and denominators for SSER)
  COALESCE(z.zendesk_tickets_created, 0) AS zendesk_tickets,
  COALESCE(k.forum_questions_posted, 0) AS forum_questions,
  COALESCE(g.kb_sessions, 0) AS kb_sessions,
  COALESCE(g.forum_question_sessions, 0) AS forum_question_sessions,
  -- SSER overall: all escalations over all self-service interactions
  SAFE_DIVIDE(
    COALESCE(z.zendesk_tickets_created, 0) + COALESCE(k.forum_questions_posted, 0),
    COALESCE(g.kb_sessions, 0) + COALESCE(g.forum_question_sessions, 0)
  ) AS sser_overall
FROM
  spine s
LEFT JOIN
  `moz-fx-data-shared-prod.sumo_metrics_derived.zendesk_tickets_base_v1` z
  ON s.event_date = z.date
  AND s.product = z.product
LEFT JOIN
  `moz-fx-data-shared-prod.sumo_metrics_derived.kitsune_questions_base_v1` k
  ON s.event_date = k.creation_date
  AND s.product = k.product
LEFT JOIN
  ga4_pivoted g
  ON s.event_date = g.event_date
  AND s.product = g.product
