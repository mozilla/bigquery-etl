-- TEMPORARY: CI probe for the sensitive-data-flow check (delete before merge).
-- Reads a contextual-services-gated stable table and writes to a broadly
-- readable derived dataset, which should trigger an advisory
-- @mozilla/dataplatform-wg review request + PR comment.
SELECT
  submission_timestamp,
  document_id
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
