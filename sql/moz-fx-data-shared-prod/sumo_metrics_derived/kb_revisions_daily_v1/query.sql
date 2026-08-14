-- Day x product x reviewer-type counts of KB revisions created, reviewed, and
-- approved, plus the daily net review flow. Derived from the revision-grain table
-- so the population and product filters stay in one place.
--
-- Built on a dense date spine (every day x every observed product x reviewer
-- type), so days with no activity report 0 rather than being absent.
--
-- `net_revisions` is a daily *flow* (created minus reviewed), not a backlog
-- level; the cumulative backlog is exposed as `running_backlog` in
-- sumo_kb_revision_kpis.
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
    UNNEST(
      GENERATE_DATE_ARRAY((SELECT start_date FROM window_start), CURRENT_DATE, INTERVAL 1 DAY)
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
