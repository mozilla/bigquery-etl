CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.content_decoding_errors`
AS
WITH events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'top_level_site') AS top_level_site
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.content_decoding_error` AS e
  CROSS JOIN
    UNNEST(e.events) AS event
  WHERE
    DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND event.category = 'network'
    AND event.name = 'content_decoding_error_report'
),
top_sites AS (
  SELECT
    top_level_site
  FROM
    events
  WHERE
    top_level_site IS NOT NULL
  GROUP BY
    top_level_site
  ORDER BY
    COUNT(*) DESC
  LIMIT
    10
),
site_counts AS (
  SELECT
    submission_date,
    events.top_level_site AS series,
    COUNT(*) AS error_count
  FROM
    events
  JOIN
    top_sites
    USING (top_level_site)
  GROUP BY
    submission_date,
    series
),
all_sites_total AS (
  SELECT
    submission_date,
    'ALL_SITES_TOTAL' AS series,
    COUNT(*) AS error_count
  FROM
    events
  GROUP BY
    submission_date
),
top_10_total AS (
  SELECT
    submission_date,
    'TOP_10_TOTAL' AS series,
    SUM(error_count) AS error_count
  FROM
    site_counts
  GROUP BY
    submission_date
)
SELECT
  *
FROM
  site_counts
UNION ALL
SELECT
  *
FROM
  all_sites_total
UNION ALL
SELECT
  *
FROM
  top_10_total
