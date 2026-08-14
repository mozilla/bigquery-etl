-- SUMO knowledge base revisions at the revision grain: when each revision was
-- created, when it was reviewed, and who reviewed it. Single source of truth for
-- KB revision throughput and review-backlog reporting.
--
-- Revision/article filters (matching the KB releases dashboard query):
--   d.parent_id IS NULL        -- original, non-translated articles
--   d.locale = 'en-US'         -- en-US articles only
--   d.is_template = FALSE      -- exclude template revisions
--   d.is_archived = FALSE      -- exclude archived articles
--   title NOT LIKE 'forum response%' -- exclude forum-response revisions
--   product NOT IN ('Thunderbird', 'Thunderbird Android') -- handled by another team
--
-- Full recompute each run over a fixed window starting 2024-01-01 (no date
-- partition parameter). `latest_document_revision_date` and the `reviewed_date`
-- backfill below are computed over the in-window revisions only, so a document
-- whose only reviewed revision predates the window start keeps a NULL
-- reviewed_date.
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
    -- Review population the revision belongs to. NULL `is_staff` means no
    -- reviewer is attached yet, i.e. the revision is still awaiting review.
    CASE
      WHEN reviewer.is_staff
        THEN 'Staff'
      WHEN NOT reviewer.is_staff
        THEN 'Community'
      ELSE 'Unreviewed'
    END AS reviewer_type
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` r
  LEFT JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus` d
    ON d.id = r.document_id
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
    AND DATE(r.created)
    BETWEEN (SELECT start_date FROM window_start)
    AND CURRENT_DATE
),
-- Assume every revision preceding a reviewed revision was covered by that same
-- review pass, so an unreviewed revision older than the document's most recent
-- review inherits that review date.
with_document_latest AS (
  SELECT
    * EXCEPT (raw_reviewed_date),
    MAX(raw_reviewed_date) OVER (PARTITION BY document_id) AS latest_document_revision_date,
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
  latest_document_revision_date,
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
