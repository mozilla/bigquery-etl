CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.customer_experience.knowledge_base_retrieval_index`
AS
SELECT
  *,
  -- Articles with no approved revision in `kitsune_wiki_revision` (last_approved_revision_date
  -- IS NULL) are treated as stale: they need editor attention regardless of "age."
  IFNULL(DATE_DIFF(CURRENT_DATE(), last_approved_revision_date, MONTH) >= 12, TRUE) AS is_stale
FROM
  `moz-fx-data-shared-prod.customer_experience_derived.knowledge_base_retrieval_index_v1`
WHERE
  metadata.embedding_succeeded
