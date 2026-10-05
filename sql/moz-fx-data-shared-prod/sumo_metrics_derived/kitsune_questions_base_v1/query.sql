WITH questions AS (
  SELECT
    DATE(TIMESTAMP(q.created_utc), "UTC") AS date_utc,
    -- Normalize product names to match other SUMO datasets
    mozfun.customer_experience.normalize_product(q.product, 'Kitsune') AS product,
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
  CURRENT_TIMESTAMP() AS generated_time
FROM
  deduped
GROUP BY
  `creation_date`,
  product
