-- Drift warning for `customer_experience.normalize_product`.
-- Scans each upstream source for raw product values that the UDF mapped to
-- 'Other' over the trailing 3 months ending at @submission_date, excluding
-- the raw values we have already decided to bucket as 'Other'. Surfaces only
-- volumes worth reviewing so isolated typos don't page anyone.
--
-- Scheduling: bqetl auto-runs checks.sql alongside a query.sql. This UDF
-- directory has no query, so wire this check into a daily monitoring query
-- (or symlink it into a consumer query directory) to get it scheduled —
-- meanwhile it is runnable ad-hoc via `./bqetl check run`.

#warn
WITH zendesk_unmapped AS (
  SELECT
    'Zendesk' AS source,
    t.custom_product AS raw_product,
    COUNT(*) AS record_count
  FROM
    `moz-fx-data-shared-prod.zendesk_syndicate.ticket` AS t
  WHERE
    DATE(t.created_at)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 3 MONTH)
    AND @submission_date
    AND t.status != 'deleted'
    AND mozfun.customer_experience.normalize_product(t.custom_product, 'Zendesk') = 'Other'
    -- raw values intentionally bucketed as 'Other'
    AND t.custom_product NOT IN ('X', 'product_other', 'pocket', 'hubs')
  GROUP BY
    raw_product
  HAVING
    record_count >= 5
),
kitsune_unmapped AS (
  SELECT
    'Kitsune' AS source,
    q.product AS raw_product,
    COUNT(*) AS record_count
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions` AS q
  WHERE
    DATE(TIMESTAMP(q.created_utc), "UTC")
    BETWEEN DATE_SUB(@submission_date, INTERVAL 3 MONTH)
    AND @submission_date
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
    record_count >= 5
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
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 3 MONTH)
    AND @submission_date
    AND ep.key = 'products'
    AND mozfun.customer_experience.normalize_product(ep.value.string_value, 'GA4') = 'Other'
    -- Exclude paths the UDF intentionally maps to 'Other'. The substring
    -- regexes mirror the GA4 'Other' branches in normalize_product; the
    -- explicit NOT IN list covers standalone exact-match 'Other' paths the
    -- substring patterns wouldn't catch.
    AND ep.value.string_value NOT IN ('/privacy-and-security/')
    AND NOT REGEXP_CONTAINS(LOWER(ep.value.string_value), r'/contributor/')
    AND NOT REGEXP_CONTAINS(
      LOWER(ep.value.string_value),
      r'/(firefox-os|firefox-lite|firefox-reality|firefox-lockwise|firefox-fire-tv|firefox-amazon-devices|firefox-send|firefox-windows-8-touch|firefox-preview|webmaker|pocket|hubs|screenshot-go|open-badges)/'
    )
  GROUP BY
    raw_product
  HAVING
    record_count >= 50
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
        'Unmapped product values in 3 months ending %t — add to normalize_product UDF: %t',
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
