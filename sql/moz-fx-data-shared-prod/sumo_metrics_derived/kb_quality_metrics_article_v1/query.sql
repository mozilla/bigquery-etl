-- KB quality metrics at the article x day grain (freshness + helpfulness).
--
-- Article filters (matching the KB quality dashboard query):
--   parent_id IS NULL          -- original, non-translated articles
--   is_archived = FALSE        -- exclude archived articles
--   category < 30              -- exclude non-user-facing articles
--   html NOT LIKE '%REDIRECT%' -- exclude decommissioned redirect articles
--   products NOT LIKE '%thunderbird%' -- Thunderbird support handled separately
--
-- Full recompute over all available history each run (no date partition parameter).
-- The date spine starts at the earliest signal in the sources (first approved
-- revision or first vote); days before an article's own first approved revision
-- produce no row, because `daily_freshness_per_article` requires
-- reviewed_date <= event_date.
WITH articles AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus`
  WHERE
    parent_id IS NULL
    AND is_archived = FALSE
    AND category < 30
    AND html NOT LIKE '%REDIRECT%'
    AND products NOT LIKE '%thunderbird%'
),
-- All-time daily vote totals per article (no date bound).
daily_votes AS (
  SELECT
    DATE(vote_created_date) AS vote_date,
    document_id,
    url,
    locale,
    SUM(CAST(helpful AS INT64)) AS helpful_votes,
    COUNT(*) AS total_votes
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.metrics_kb_votes_details`
  GROUP BY
    vote_date,
    document_id,
    url,
    locale
),
-- Earliest date with any signal: first approved revision or first vote.
-- MIN ignores NULLs, so this survives either source being empty.
window_start AS (
  SELECT
    MIN(d) AS start_date
  FROM
    UNNEST(
      [
        (SELECT MIN(vote_date) FROM daily_votes),
        (
          SELECT
            MIN(DATE(reviewed))
          FROM
            `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision`
          WHERE
            is_approved
        )
      ]
    ) AS d
),
-- Daily date spine x each article, covering all history.
dates AS (
  SELECT
    document_id,
    event_date
  FROM
    (SELECT DISTINCT id AS document_id FROM articles) AS columns
  CROSS JOIN
    UNNEST(
      GENERATE_DATE_ARRAY(
        (SELECT start_date FROM window_start),
        DATE(CURRENT_DATE - INTERVAL 1 DAY),
        INTERVAL 1 DAY
      )
    ) AS event_date
),
-- Approved revisions for the in-scope articles.
revisions AS (
  SELECT
    r.document_id,
    DATE(r.reviewed) AS reviewed_date,
    d.share_link,
    d.locale
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` r
  LEFT JOIN
    articles d
    ON r.document_id = d.id
  WHERE
    r.is_approved = TRUE
    AND d.products NOT LIKE '%thunderbird%'
),
-- For each day, days since the article's most recent approved revision.
daily_freshness_per_article AS (
  SELECT
    event_date,
    document_id,
    share_link,
    locale,
    last_revision_date,
    DATE_DIFF(event_date, last_revision_date, DAY) AS days_since_last_revision
  FROM
    (
      SELECT
        d.event_date,
        d.document_id,
        r.share_link,
        r.locale,
        MAX(r.reviewed_date) AS last_revision_date
      FROM
        dates d
      JOIN
        revisions r
        ON d.document_id = r.document_id
      WHERE
        r.reviewed_date <= d.event_date
      GROUP BY
        d.event_date,
        d.document_id,
        r.share_link,
        r.locale
    )
),
-- For each day, the daily and running cumulative helpfulness votes per article.
-- The spine starts at or before the first vote, so the running totals are
-- cumulative over all history without needing a pre-window seed.
daily_helpfulness_per_article AS (
  SELECT
    dates.event_date,
    dates.document_id,
    help.url,
    help.locale,
    help.helpful_votes,
    help.total_votes,
    SUM(COALESCE(help.helpful_votes, 0)) OVER (
      PARTITION BY
        dates.document_id
      ORDER BY
        dates.event_date
    ) AS running_helpful_votes,
    SUM(COALESCE(help.total_votes, 0)) OVER (
      PARTITION BY
        dates.document_id
      ORDER BY
        dates.event_date
    ) AS running_total_votes
  FROM
    dates
  LEFT JOIN
    daily_votes help
    ON dates.event_date = help.vote_date
    AND dates.document_id = help.document_id
),
-- Article-level daily metrics with product mapping applied.
main AS (
  SELECT
    f.event_date,
    f.document_id,
    articles.locale,
    articles.slug,
    articles.products,
    -- Canonical CX product name. `products` is a slash-delimited path, so the
    -- 'GA4' rule set (which matches path-style inputs) applies. Unmapped paths
    -- return 'Other' and are filtered out in the final SELECT.
    mozfun.customer_experience.normalize_product(articles.products, 'GA4') AS product,
    f.last_revision_date,
    f.days_since_last_revision,
    -- "fresh" = approved revision within the last 365 days
    IF(f.days_since_last_revision <= 365, 1, 0) AS fresh_ind,
    h.url,
    articles.share_link,
    h.helpful_votes,
    h.total_votes,
    h.running_helpful_votes,
    h.running_total_votes
  FROM
    daily_freshness_per_article f
  LEFT JOIN
    daily_helpfulness_per_article h
    ON f.event_date = h.event_date
    AND f.document_id = h.document_id
  LEFT JOIN
    articles
    ON f.document_id = articles.id
  WHERE
    articles.locale = 'en-US'
)
SELECT
  *
FROM
  main
-- Drop articles whose product path did not map to a canonical CX product.
WHERE
  product != 'Other'
