-- Drift warnings for the `customer_experience.normalize_product` mapping this
-- table depends on. `normalize_product` returns 'Other' for unknown inputs
-- rather than leaking the raw value, so a new `products` path added in Kitsune
-- changes the product breakdown silently. It also decides which revisions are
-- excluded altogether, because the Thunderbird filter keys off the UDF's output,
-- so a new Thunderbird-tagged path drops revisions from the table with no signal.
--
-- Both checks mirror kb_revisions_base_v1's own population filters so they only
-- flag paths that actually reach the table. The table is a full recompute with no
-- date partition, so these scan the whole window rather than @submission_date.

#warn
-- 1. Novel `products` paths that fall through to 'Other'.
WITH scoped_documents AS (
  SELECT
    d.products,
    COUNT(*) AS revisions
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` AS r
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus` AS d
    ON d.id = r.document_id
  WHERE
    d.parent_id IS NULL
    AND d.locale = 'en-US'
    AND d.is_template = FALSE
    AND d.is_archived = FALSE
    AND LOWER(d.title) NOT LIKE 'forum response%'
    AND DATE(r.created) >= DATE '2024-01-01'
  GROUP BY
    d.products
),
unmapped AS (
  SELECT
    products,
    revisions
  FROM
    scoped_documents
  WHERE
    COALESCE(mozfun.customer_experience.normalize_product(products, 'GA4'), 'Other') = 'Other'
    -- Paths the UDF intentionally maps to 'Other'. These mirror the GA4 'Other'
    -- branches in normalize_product. Keep them in sync when the UDF changes.
    -- Articles with no product assigned are a known state, not drift.
    AND products IS NOT NULL
    AND products != ''
    AND products NOT IN ('/privacy-and-security/')
    AND NOT REGEXP_CONTAINS(LOWER(products), r'/contributor/')
    AND NOT REGEXP_CONTAINS(
      LOWER(products),
      r'/(firefox-os|firefox-lite|firefox-reality|firefox-lockwise|firefox-fire-tv|firefox-amazon-devices|firefox-send|firefox-windows-8-touch|firefox-preview|webmaker|pocket|hubs|screenshot-go|open-badges)/'
    )
)
SELECT
  IF(
    (SELECT COUNT(*) FROM unmapped) > 0,
    ERROR(
      FORMAT(
        'Unmapped KB product paths — add to normalize_product UDF: %t',
        ARRAY(SELECT AS STRUCT products, revisions FROM unmapped ORDER BY revisions DESC)
      )
    ),
    NULL
  );

#warn
-- 2. Novel `products` paths excluded by the Thunderbird filter. The enumerated
-- list is the set as of 2026-08-14. A new entry means revisions are being
-- dropped from this table that were not dropped before, which is worth a look
-- because multi-product paths carrying a Thunderbird tag are not necessarily
-- Thunderbird-team content.
WITH thunderbird_excluded AS (
  SELECT
    d.products,
    COUNT(*) AS revisions
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` AS r
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document_plus` AS d
    ON d.id = r.document_id
  WHERE
    d.parent_id IS NULL
    AND d.locale = 'en-US'
    AND d.is_template = FALSE
    AND d.is_archived = FALSE
    AND LOWER(d.title) NOT LIKE 'forum response%'
    AND DATE(r.created) >= DATE '2024-01-01'
    AND mozfun.customer_experience.normalize_product(d.products, 'GA4') IN (
      'Thunderbird',
      'Thunderbird Android'
    )
    AND d.products NOT IN (
      '/thunderbird/',
      '/thunderbird-android/',
      '/thunderbird/thunderbird-android/',
      '/firefox/ios/mobile/thunderbird/',
      '/firefox/thunderbird/'
    )
  GROUP BY
    d.products
)
SELECT
  IF(
    (SELECT COUNT(*) FROM thunderbird_excluded) > 0,
    ERROR(
      FORMAT(
        'New Thunderbird-mapped KB product paths are being excluded from kb_revisions_base_v1 — confirm they are Thunderbird-team content: %t',
        ARRAY(
          SELECT AS STRUCT
            products,
            revisions
          FROM
            thunderbird_excluded
          ORDER BY
            revisions DESC
        )
      )
    ),
    NULL
  );
