CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_kb_revision_kpis`
AS
-- Day-level SUMO KB revision KPIs: total revisions created, reviewed, and
-- approved across all products and reviewer types, their 14-day moving averages,
-- the same moving averages lagged 14 days for period-over-period comparison, and
-- the cumulative review backlog.
--
-- Rolled up from kb_revisions_daily_v1 so the population, product, and reviewer
-- definitions stay in one place. Join back to kb_revisions_daily_v1 on
-- `event_date` to break the totals down by product or reviewer type.
--
--   total_backlog   : daily net flow (created - reviewed) — can be negative
--   running_backlog : cumulative net flow to date, i.e. the actual backlog level
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
  ) AS running_backlog
FROM
  moving_averages
