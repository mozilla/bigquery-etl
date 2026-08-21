CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.market_intel.market_research_jobs_retrieval_index`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.market_intel_derived.market_research_jobs_retrieval_index_v1`
WHERE
  metadata.embedding_succeeded
