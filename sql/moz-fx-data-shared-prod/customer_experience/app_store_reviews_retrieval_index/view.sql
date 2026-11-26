CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.app_store_reviews_retrieval_index`
AS
SELECT
  TIMESTAMP_TRUNC(review.created_date, SECOND) AS creation_date,
  review.id,
  review.app_id,
  review.territory,
  review.rating,
  review.reviewer_nickname,
  app_version.version_string AS app_version,
  review.title,
  review.body,
  idx.review_summary_llm,
  idx.review_category_llm,
  idx.review_language_llm,
  idx.review_entities_llm,
  idx.review_topics_llm,
  idx.review_sentiment_score,
  idx.embedding
FROM
  `moz-fx-data-shared-prod.app_store_v2_syndicate.app_store_review` AS review
LEFT JOIN
  `moz-fx-data-shared-prod.customer_experience_derived.app_store_reviews_retrieval_index_v1` AS idx
  ON idx.id = review.id
LEFT JOIN
  `moz-fx-data-shared-prod.app_store_v2_syndicate.app_store_app_version_review` AS version_review
  ON version_review.review_id = review.id
LEFT JOIN
  `moz-fx-data-shared-prod.app_store_v2_syndicate.app_store_app_version` AS app_version
  ON app_version.id = version_review.app_version_id
