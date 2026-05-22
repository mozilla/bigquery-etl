-- Drift warning for `customer_experience.normalize_product`.
-- Scans each upstream source for raw product values that the UDF mapped to
-- 'Other' on @submission_date, excluding raw values we have already decided
-- to bucket as 'Other'. Runs daily alongside the consumer query so new slugs
-- surface as they first appear — no need to re-scan history each run.

#warn
WITH zendesk_unmapped AS (
  SELECT
    'Zendesk' AS source,
    t.custom_product AS raw_product,
    COUNT(*) AS record_count
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket` AS t
  WHERE
    DATE(t.created_at) = @submission_date
    AND mozfun.customer_experience.normalize_product(t.custom_product, 'Zendesk') = 'Other'
    -- raw values intentionally bucketed as 'Other'
    AND t.custom_product NOT IN ('X', 'product_other', 'pocket', 'hubs')
  GROUP BY
    raw_product
  HAVING
    record_count >= 1
),
kitsune_unmapped AS (
  SELECT
    'Kitsune' AS source,
    q.product AS raw_product,
    COUNT(*) AS record_count
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions` AS q
  WHERE
    DATE(TIMESTAMP(q.created_utc), "UTC") = @submission_date
    AND mozfun.customer_experience.normalize_product(q.product, 'Kitsune') = 'Other'
    AND q.product NOT IN (
      'firefox-os',
      'firefox-lite',
      'firefox-lockwise',
      'firefox-reality',
      'firefox-fire-tv',
      'firefox-amazon-devices',
      'firefox-windows-8-touch',
      'webmaker',
      'hubs'
    )
  GROUP BY
    raw_product
  HAVING
    record_count >= 1
),
ga4_unmapped AS (
  SELECT
    'GA4' AS source,
    ep.value.string_value AS raw_product,
    COUNT(*) AS record_count
  FROM
    `mozdata.sumo_ga.ga4_events`,
    UNNEST(event_params) AS ep
  WHERE
    submission_date = @submission_date
    AND ep.key = 'products'
    AND mozfun.customer_experience.normalize_product(ep.value.string_value, 'GA4') = 'Other'
    -- Exclude paths the UDF intentionally maps to 'Other'. The substring
    -- regexes mirror the GA4 'Other' branches in normalize_product; the
    -- explicit NOT IN list covers standalone exact-match 'Other' paths the
    -- substring patterns wouldn't catch.
    AND ep.value.string_value NOT IN ('/privacy-and-security/')
    -- Drop malformed paths (consecutive slashes) — instrumentation noise, not a product signal
    AND NOT REGEXP_CONTAINS(ep.value.string_value, r'//')
    AND NOT REGEXP_CONTAINS(LOWER(ep.value.string_value), r'/contributor/')
    AND NOT REGEXP_CONTAINS(
      LOWER(ep.value.string_value),
      r'/(firefox-os|firefox-lite|firefox-reality|firefox-lockwise|firefox-fire-tv|firefox-amazon-devices|firefox-send|firefox-windows-8-touch|firefox-preview|webmaker|pocket|hubs|screenshot-go|open-badges)/'
    )
  GROUP BY
    raw_product
  HAVING
    record_count >= 5
),
all_unmapped AS (
  SELECT
    *
  FROM
    zendesk_unmapped
  UNION ALL
  SELECT
    *
  FROM
    kitsune_unmapped
  UNION ALL
  SELECT
    *
  FROM
    ga4_unmapped
)
SELECT
  IF(
    (SELECT COUNT(*) FROM all_unmapped) > 0,
    ERROR(
      FORMAT(
        'Unmapped product values on %t — add to normalize_product UDF: %t',
        @submission_date,
        ARRAY(
          SELECT AS STRUCT
            source,
            raw_product,
            record_count
          FROM
            all_unmapped
          ORDER BY
            record_count DESC
        )
      )
    ),
    NULL
  );
