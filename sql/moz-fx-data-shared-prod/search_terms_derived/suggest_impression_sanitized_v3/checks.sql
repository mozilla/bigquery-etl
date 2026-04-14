#fail
SELECT
  IF(
    COUNT(*) = 0,
    ERROR("Merino sanitization job has not completed successfully for today. Will retry."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata_v2`
WHERE
  DATE(started_at) = CURRENT_DATE()
  AND status = 'SUCCESS';
