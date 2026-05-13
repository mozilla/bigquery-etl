CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.knowledgebase_retrieval_index`
AS
SELECT
  *,
  EXP(-DATE_DIFF(CURRENT_DATE(), DATE(last_updated), DAY) / 30) AS recency_score
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.knowledgebase_retrieval_index_v1`
WHERE
  metadata.embedding_succeeded
