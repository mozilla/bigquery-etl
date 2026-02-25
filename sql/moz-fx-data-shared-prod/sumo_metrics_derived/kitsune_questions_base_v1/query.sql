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
    `moz-fx-sumo-prod.sumo_syndicate.kitsune_questions` q
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
  date_pst AS `date`,
  product,
  COUNT(*) AS forum_questions_posted,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  deduped
GROUP BY
  `date`,
  product
