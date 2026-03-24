WITH questions AS (
  SELECT
    DATE(TIMESTAMP(q.created_utc), "UTC") AS date_utc,
    -- Normalize product names to match other SUMO datasets
    COALESCE(m.product_mapping, q.product) AS product,
    q.question_id
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions` q
  LEFT JOIN
    `moz-fx-data-shared-prod.static.cx_product_mappings_v1` m
    ON m.product = q.product
    AND m.source = 'Kitsune'
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
