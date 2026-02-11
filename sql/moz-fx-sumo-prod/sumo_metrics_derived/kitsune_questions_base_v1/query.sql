-- Kitsune Forum Questions Base Table (Daily Grain)
-- Single source of truth for SUMO forum questions posted
--
-- This table tracks at DAILY granularity:
-- - Forum questions posted each day (from Kitsune database)
-- - Breakdown by product
--
-- Replaces duplicated q2 CTEs in SSSR scripts
--
-- Grain: Daily x Product
WITH questions AS (
  SELECT
    DATE(TIMESTAMP(q.created_utc), "America/Los_Angeles") AS date_pst,
      -- Normalize product names to match other SUMO datasets
    CASE
      q.product
      WHEN 'mobile'
        THEN 'firefox-android'
      WHEN 'ios'
        THEN 'firefox-ios'
      WHEN 'firefox-enterprise'
        THEN 'firefox'
      ELSE q.product
    END AS product,
    q.question_id
  FROM
    `moz-fx-sumo-prod.sumo.kitsune_questions` q
  WHERE
    DATE(TIMESTAMP(q.created_utc), "America/Los_Angeles")
    BETWEEN '2024-07-01'
    AND '2026-01-05'
),
deduped AS (
  SELECT
    date_pst,
    question_id,
    product
  FROM
    questions
  GROUP BY
    date_pst,
    question_id,
    product
)
SELECT
  date_pst AS date,
  product,
  COUNT(*) AS forum_questions_posted,
  COUNT(DISTINCT question_id) AS unique_questions,
  EXTRACT(YEAR FROM date_pst) AS year,
  EXTRACT(MONTH FROM date_pst) AS month,
  EXTRACT(WEEK FROM date_pst) AS week_number,
  EXTRACT(DAYOFWEEK FROM date_pst) AS day_of_week,
  FORMAT_DATE('%Y-%m', date_pst) AS year_month,
  FORMAT_DATE('%Y-W%W', date_pst) AS year_week,
  FORMAT_DATE('%A', date_pst) AS day_name,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  deduped
GROUP BY
  date,
  product
