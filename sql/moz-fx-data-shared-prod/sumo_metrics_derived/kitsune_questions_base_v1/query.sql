WITH questions AS (
  SELECT
    DATE(TIMESTAMP(q.created_utc), "UTC") AS date_utc,
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
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions` q
),
deduped AS (
  SELECT
    date_utc,
    question_id,
    product
  FROM
    questions
  GROUP BY
    date_utc,
    question_id,
    product
)
SELECT
  date_utc AS `creation_date`,
  product,
  COUNT(*) AS forum_questions_posted,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  deduped
GROUP BY
  `creation_date`,
  product
