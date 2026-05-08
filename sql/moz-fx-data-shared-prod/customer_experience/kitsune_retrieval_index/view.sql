CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.kitsune_retrieval_index`
AS
SELECT
  *,
  EXP(-DATE_DIFF(CURRENT_DATE(), creation_date, DAY) / 30) AS recency_score
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.kitsune_retrieval_index_v1`
WHERE
  metadata.embedding_succeeded
-- test change
