CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.app_store_reviews_retrieval_index`
AS
SELECT
  TIMESTAMP_TRUNC(review.created_date, SECOND) AS creation_date,
  review.id,
  review.app_id,
  review.territory,
  review.rating,
  review.title,
  review.body,
  idx.* EXCEPT (creation_date, id, title, body)
FROM
  `moz-fx-data-shared-prod.app_store_v2_syndicate.app_store_review` AS review
LEFT JOIN
  `moz-fx-data-shared-prod.customer_experience_derived.app_store_reviews_retrieval_index_v1` AS idx
  -- The JOIN on creation_date keeps one row per review ID.
  ON idx.id = review.id
  AND idx.creation_date = DATE(review.created_date)
