-- SUMO knowledge base revisions at the revision grain: when each revision was
-- created, when it was reviewed, and who reviewed it. Single source of truth for
-- KB revision throughput and review-backlog reporting.
--
-- Revision/article filters (matching the KB releases dashboard query):
--   d.parent_id IS NULL        -- original, non-translated articles (the join to
--                              -- d is inner, so this cannot mean "no document")
--   d.locale = 'en-US'         -- en-US articles only
--   d.is_template = FALSE      -- exclude template revisions
--   d.is_archived = FALSE      -- exclude archived articles
--   title NOT LIKE 'forum response%' -- exclude forum-response revisions
--   product NOT IN ('Thunderbird', 'Thunderbird Android') -- handled by another team
--
-- Full recompute each run over a fixed window starting 2024-01-01 (no date
-- partition parameter). `latest_document_reviewed_date` and the `reviewed_date`
-- backfill below are computed over the in-window revisions only, so a document
-- whose only reviewed revision predates the window start keeps a NULL
-- reviewed_date.
--
-- The window filters on `r.created` only, so a revision created before the window
-- start but reviewed inside it is absent entirely (21 revisions to date). That
-- means `reviewed_revisions` / `approved_revisions` in kb_revisions_daily_v1
-- understate review throughput for dates early in the window -- an early-2024
-- pass that cleared a 2023 backlog is invisible -- and the understatement decays
-- as the window ages. Do not compare early-2024 throughput against later dates as
-- if they were on the same footing. Filtering on `created OR reviewed` would fix
-- this, but it would also diverge from the upstream SUMO dashboard that this
-- table was validated against, so it is left as a known limitation.
WITH window_start AS (
  SELECT
    DATE '2024-01-01' AS start_date
),
revisions AS (
  SELECT
    r.id AS revision_id,
    r.document_id,
    d.title,
    d.slug,
    'https://support.mozilla.org/en-US/kb/' || d.slug AS article_link,
    'https://support.mozilla.org/en-US/kb/' || d.slug || '/revision/' || r.id AS revision_link,
    d.locale,
    -- Canonical CX product name. `products` is a slash-delimited path, so the
    -- 'GA4' rule set (which matches path-style inputs) applies. Unmapped paths
    -- return 'Other'; a NULL path (the article has no product assigned) returns
    -- NULL, which is folded into 'Other' so those revisions survive the
    -- Thunderbird filter below and still count toward review throughput.
    COALESCE(mozfun.customer_experience.normalize_product(d.products, 'GA4'), 'Other') AS product,
    DATE(r.created) AS created_date,
    -- Cover revisions that are approved but have a NULL `reviewed`; that happens
    -- when several preceding revisions are ignored in one review pass.
    IF(
      r.reviewed IS NULL
      AND r.is_approved,
      DATE(r.created),
      DATE(r.reviewed)
    ) AS raw_reviewed_date,
    r.is_approved,
    r.creator_id,
    creator.username AS creator_username,
    creator.is_staff AS creator_is_staff,
    r.reviewer_id,
    reviewer.username AS reviewer_username,
    -- Review population the revision belongs to. Keyed off `reviewer_id`, which
    -- is what actually carries "nobody has picked this up yet" — a NULL
    -- `reviewer.is_staff` cannot distinguish that from a reviewer_id whose user
    -- row is missing from `kitsune_auth_user` (deleted or unsynced account), so
    -- that case gets its own 'Unknown' label rather than being folded into
    -- 'Unreviewed'. No revision is in that state today; the branch exists so a
    -- future join miss shows up instead of inflating the unreviewed bucket.
    CASE
      WHEN r.reviewer_id IS NULL
        THEN 'Unreviewed'
      WHEN reviewer.is_staff
        THEN 'Staff'
      WHEN NOT reviewer.is_staff
        THEN 'Community'
      ELSE 'Unknown'
    END AS reviewer_type
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` r
  -- Inner join: a revision whose document is missing is out of scope, and the
  -- d.* filters below would discard it anyway.
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus` d
    ON d.id = r.document_id
  -- These stay LEFT: reviewer_id is NULL for revisions awaiting review.
  LEFT JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_auth_user` creator
    ON r.creator_id = creator.id
  LEFT JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_auth_user` reviewer
    ON r.reviewer_id = reviewer.id
  WHERE
    d.parent_id IS NULL
    AND d.locale = 'en-US'
    AND d.is_template = FALSE
    AND d.is_archived = FALSE
    AND LOWER(d.title) NOT LIKE 'forum response%'
    -- Ends at yesterday, not CURRENT_DATE: today is still accumulating, and a
    -- partial day would bias the last point of every moving average downstream.
    -- Matches kb_quality_metrics_article_v1's last-complete-day convention.
    AND DATE(r.created)
    BETWEEN (SELECT start_date FROM window_start)
    AND DATE(CURRENT_DATE - INTERVAL 1 DAY)
),
-- Assume every revision preceding a reviewed revision was covered by that same
-- review pass, so an unreviewed revision older than the document's most recent
-- review inherits that review date.
with_document_latest AS (
  SELECT
    * EXCEPT (raw_reviewed_date),
    MAX(raw_reviewed_date) OVER (PARTITION BY document_id) AS latest_document_reviewed_date,
    IF(
      raw_reviewed_date IS NULL
      AND created_date <= MAX(raw_reviewed_date) OVER (PARTITION BY document_id),
      MAX(raw_reviewed_date) OVER (PARTITION BY document_id),
      raw_reviewed_date
    ) AS reviewed_date
  FROM
    revisions
)
SELECT
  revision_id,
  document_id,
  title,
  slug,
  article_link,
  revision_link,
  locale,
  product,
  created_date,
  reviewed_date,
  latest_document_reviewed_date,
  is_approved,
  creator_id,
  creator_username,
  creator_is_staff,
  reviewer_id,
  reviewer_username,
  reviewer_type
FROM
  with_document_latest
-- Thunderbird KB articles are maintained by a separate team.
WHERE
  product NOT IN ('Thunderbird', 'Thunderbird Android')
