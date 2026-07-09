SELECT
  DATE(submission_timestamp) AS submission_date,
  country,
  form_factor,
  advertiser,
  LOWER(TRIM(query)) AS partial,
  position,
  match_type,
  experiments,
  is_clicked,
FROM
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v3`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND session_id IS NOT NULL
  AND query IS NOT NULL
  AND LENGTH(TRIM(query)) > 0
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY session_id, advertiser ORDER BY sequence_no DESC, request_id) = 1
