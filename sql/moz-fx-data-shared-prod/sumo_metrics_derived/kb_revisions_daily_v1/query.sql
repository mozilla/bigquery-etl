-- Day x product x reviewer-type counts of KB revisions created, reviewed, and
-- approved, plus the daily net review flow. Derived from the revision-grain table
-- so the population and product filters stay in one place.
--
-- Built on a dense date spine (every day x every observed product x reviewer
-- type), so days with no activity report 0 rather than being absent.
--
-- `net_revisions` is a daily *flow* (created minus reviewed), not a backlog
-- level; the cumulative version is `backlog_change_since_window_start` in
-- sumo_kb_revision_kpis, which is also a trend rather than a pending count.
--
-- Caveat on the reviewer_type split of `created_revisions` (and therefore of
-- `net_revisions`): reviewer_type is a property of the *review*, not of anything
-- known when the revision was created, and this table is recomputed in full each
-- run. So a revision created today counts under 'Unreviewed' for its creation
-- date now, and moves to 'Staff'/'Community' for that same date once it is
-- reviewed. The recent tail is materially affected — about 27% of revisions
-- created in the last 30 days are still unreviewed — so a created-by-reviewer-
-- type series redistributes between buckets on every run. Day-level totals are
-- stable; only the split is retroactively mutable. Kept at this grain because
-- the upstream SUMO dashboard groups creations the same way.
WITH base AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.kb_revisions_base_v1`
),
window_start AS (
  SELECT
    MIN(created_date) AS start_date
  FROM
    base
),
-- Every combination of date, product, and reviewer type.
dates AS (
  SELECT
    product,
    reviewer_type,
    event_date
  FROM
    (SELECT DISTINCT product, reviewer_type FROM base) AS columns
  CROSS JOIN
    -- Ends at yesterday to match the upstream window: today is still
    -- accumulating, and a partial final day would bias the last point of every
    -- moving average in sumo_kb_revision_kpis.
    UNNEST(
      GENERATE_DATE_ARRAY(
        (SELECT start_date FROM window_start),
        DATE(CURRENT_DATE - INTERVAL 1 DAY),
        INTERVAL 1 DAY
      )
    ) AS event_date
),
created_revisions AS (
  SELECT
    created_date,
    product,
    reviewer_type,
    COUNT(*) AS created_revisions
  FROM
    base
  GROUP BY
    created_date,
    product,
    reviewer_type
),
reviewed_revisions AS (
  SELECT
    reviewed_date,
    product,
    reviewer_type,
    COUNT(*) AS reviewed_revisions
  FROM
    base
  WHERE
    reviewed_date IS NOT NULL
  GROUP BY
    reviewed_date,
    product,
    reviewer_type
),
approved_revisions AS (
  SELECT
    reviewed_date AS approved_date,
    product,
    reviewer_type,
    COUNT(*) AS approved_revisions
  FROM
    base
  WHERE
    reviewed_date IS NOT NULL
    AND is_approved
  GROUP BY
    approved_date,
    product,
    reviewer_type
)
SELECT
  dates.event_date,
  DATE_TRUNC(dates.event_date, WEEK) AS week,
  dates.product,
  dates.reviewer_type,
  COALESCE(created_revisions.created_revisions, 0) AS created_revisions,
  COALESCE(reviewed_revisions.reviewed_revisions, 0) AS reviewed_revisions,
  COALESCE(approved_revisions.approved_revisions, 0) AS approved_revisions,
  COALESCE(created_revisions.created_revisions, 0) - COALESCE(
    reviewed_revisions.reviewed_revisions,
    0
  ) AS net_revisions
FROM
  dates
LEFT JOIN
  created_revisions
  ON dates.event_date = created_revisions.created_date
  AND dates.product = created_revisions.product
  AND dates.reviewer_type = created_revisions.reviewer_type
LEFT JOIN
  reviewed_revisions
  ON dates.event_date = reviewed_revisions.reviewed_date
  AND dates.product = reviewed_revisions.product
  AND dates.reviewer_type = reviewed_revisions.reviewer_type
LEFT JOIN
  approved_revisions
  ON dates.event_date = approved_revisions.approved_date
  AND dates.product = approved_revisions.product
  AND dates.reviewer_type = approved_revisions.reviewer_type
