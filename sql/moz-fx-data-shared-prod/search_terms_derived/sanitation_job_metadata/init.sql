CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.sanitation_job_metadata`
AS
SELECT
  *
FROM
  `mozdata.search_terms_unsanitized_analysis.prototype_sanitation_job_metadata`
WHERE
  DATE(timestamp) >= (CURRENT_DATE() - 14)
