#fail
SELECT
  IF(
    COUNT(*) = 0,
    ERROR(
      "Merino sanitization job has not completed successfully for today. Wait for sanitization job to complete and re-run parent task."
    ),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata_v2`
WHERE
  DATE(started_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND status = 'SUCCESS';
