CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_kb_revision_kpis`
AS
-- Day-level SUMO KB revision KPIs: total revisions created, reviewed, and
-- approved across all products and reviewer types, their 14-day moving averages,
-- the same moving averages lagged 14 days for period-over-period comparison, and
-- the cumulative change in review backlog since the upstream window start.
--
-- Rolled up from kb_revisions_daily_v1 so the population, product, and reviewer
-- definitions stay in one place. Join back to kb_revisions_daily_v1 on
-- `event_date` to break the totals down by product or reviewer type.
--
--   total_backlog : daily net flow (created - reviewed) — can be negative
--   backlog_change_since_window_start : cumulative net flow since the upstream
--     window start, NOT the pending-review count. kb_revisions_base_v1 only
--     contains revisions created on or after 2024-01-01, so the ~3,400 older
--     revisions that are still unreviewed are absent from this sum, and it can
--     go negative on any date where cumulative reviews since the window start
--     exceed cumulative creations. Read it as a trend, not a level. A true
--     all-time level would require widening the upstream window to cover every
--     revision rather than seeding a constant here, because pre-window
--     revisions do still get reviewed inside the window (21 to date), so a
--     fixed seed would never decay.
WITH day_level AS (
  SELECT
    event_date,
    SUM(created_revisions) AS total_created_revisions,
    SUM(reviewed_revisions) AS total_reviewed_revisions,
    SUM(approved_revisions) AS total_approved_revisions
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.kb_revisions_daily_v1`
  GROUP BY
    event_date
),
moving_averages AS (
  SELECT
    *,
    AVG(total_created_revisions) OVER (
      ORDER BY
        event_date
      ROWS BETWEEN
        13 PRECEDING
        AND CURRENT ROW
    ) AS total_created_revisions_14_ma,
    AVG(total_reviewed_revisions) OVER (
      ORDER BY
        event_date
      ROWS BETWEEN
        13 PRECEDING
        AND CURRENT ROW
    ) AS total_reviewed_revisions_14_ma,
    AVG(total_approved_revisions) OVER (
      ORDER BY
        event_date
      ROWS BETWEEN
        13 PRECEDING
        AND CURRENT ROW
    ) AS total_approved_revisions_14_ma
  FROM
    day_level
)
SELECT
  *,
  LAG(total_created_revisions_14_ma, 14) OVER (
    ORDER BY
      event_date
  ) AS lagged_total_created_revisions_14_ma,
  LAG(total_reviewed_revisions_14_ma, 14) OVER (
    ORDER BY
      event_date
  ) AS lagged_total_reviewed_revisions_14_ma,
  LAG(total_approved_revisions_14_ma, 14) OVER (
    ORDER BY
      event_date
  ) AS lagged_total_approved_revisions_14_ma,
  total_created_revisions - total_reviewed_revisions AS total_backlog,
  SUM(total_created_revisions - total_reviewed_revisions) OVER (
    ORDER BY
      event_date
  ) AS backlog_change_since_window_start
FROM
  moving_averages
