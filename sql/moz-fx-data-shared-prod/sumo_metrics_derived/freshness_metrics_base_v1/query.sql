WITH date_range AS (
  -- Last 90 days of data
  SELECT
    DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AS start_date,
    CURRENT_DATE() AS end_date
),
-- Generate daily date spine
daily_dates AS (
  SELECT
    `date` AS observation_date
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY((SELECT start_date FROM date_range), (SELECT end_date FROM date_range))
    ) AS date
),
-- For each document, find the earliest approved revision (article creation date)
first_revisions AS (
  SELECT
    document_id,
    MIN(DATE(reviewed)) AS first_approved_date
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision`
  WHERE
    is_approved = TRUE
  GROUP BY
    document_id
),
-- For each document, find the most recent approved revision before each observation date
latest_revisions AS (
  SELECT
    dd.observation_date,
    wr.document_id,
    MAX(DATE(wr.reviewed)) AS latest_revision_date
  FROM
    daily_dates dd
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_revision` wr
    ON wr.is_approved = TRUE
    AND DATE(wr.reviewed) <= dd.observation_date
  GROUP BY
    dd.observation_date,
    wr.document_id
),
-- Count total articles that exist as of each date
total_articles_daily AS (
  SELECT
    dd.observation_date AS event_date,
    COUNT(DISTINCT d.id) AS total_articles,
    COUNT(
      DISTINCT
      CASE
        WHEN lr.latest_revision_date >= DATE_SUB(dd.observation_date, INTERVAL 6 MONTH)
          THEN d.id
      END
    ) AS articles_updated_within_6_months,
    -- Age buckets
    COUNT(
      DISTINCT
      CASE
        WHEN lr.latest_revision_date >= DATE_SUB(dd.observation_date, INTERVAL 3 MONTH)
          THEN d.id
      END
    ) AS articles_0_3_months,
    COUNT(
      DISTINCT
      CASE
        WHEN lr.latest_revision_date >= DATE_SUB(dd.observation_date, INTERVAL 6 MONTH)
          AND lr.latest_revision_date < DATE_SUB(dd.observation_date, INTERVAL 3 MONTH)
          THEN d.id
      END
    ) AS articles_3_6_months,
    COUNT(
      DISTINCT
      CASE
        WHEN lr.latest_revision_date >= DATE_SUB(dd.observation_date, INTERVAL 12 MONTH)
          AND lr.latest_revision_date < DATE_SUB(dd.observation_date, INTERVAL 6 MONTH)
          THEN d.id
      END
    ) AS articles_6_12_months,
    COUNT(
      DISTINCT
      CASE
        WHEN lr.latest_revision_date < DATE_SUB(dd.observation_date, INTERVAL 12 MONTH)
          THEN d.id
      END
    ) AS articles_12_plus_months
  FROM
    daily_dates dd
  JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_wiki_document` d
    ON d.locale = 'en-US'
    AND d.is_archived = FALSE
  JOIN
    first_revisions fr
    ON fr.document_id = d.id
    -- Article must have been created before the observation date
    AND fr.first_approved_date <= dd.observation_date
  LEFT JOIN
    latest_revisions lr
    ON lr.observation_date = dd.observation_date
    AND lr.document_id = d.id
  GROUP BY
    dd.observation_date
),
metrics AS (
  SELECT
    *,
    total_articles - articles_updated_within_6_months AS outdated_articles,
    ROUND(
      CAST(articles_updated_within_6_months AS NUMERIC) / NULLIF(total_articles, 0) * 100,
      2
    ) AS freshness_percentage
  FROM
    total_articles_daily
)
SELECT
  event_date,
  total_articles,
  articles_updated_within_6_months,
  outdated_articles,
  articles_0_3_months,
  articles_3_6_months,
  articles_6_12_months,
  articles_12_plus_months,
  freshness_percentage,
  CASE
    WHEN freshness_percentage >= 80
      THEN 'excellent'
    WHEN freshness_percentage >= 60
      THEN 'good'
    WHEN freshness_percentage >= 40
      THEN 'fair'
    ELSE 'poor'
  END AS freshness_tier,
  CASE
    WHEN freshness_percentage >= 75
      THEN TRUE
    ELSE FALSE
  END AS meets_75pct_target,
  CASE
    WHEN freshness_percentage >= 90
      THEN TRUE
    ELSE FALSE
  END AS meets_90pct_stretch_goal,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  metrics
