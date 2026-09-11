-- TEMPORARY: negative-control CI probe for the sensitive-data-flow check
-- (delete before merge). Reads a broadly-readable source (telemetry_stable is
-- granted to workgroup:mozilla-confidential/data-viewers) into a broadly
-- readable derived dataset, so there is no widening and it should NOT be flagged.
SELECT
  submission_timestamp,
  document_id
FROM
  `moz-fx-data-shared-prod.telemetry_stable.main_v5`
