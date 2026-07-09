WITH deduped AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    country,
    form_factor,
    advertiser,
    LOWER(TRIM(query)) AS partial,
    position,
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
    ROW_NUMBER() OVER (PARTITION BY session_id, advertiser ORDER BY sequence_no DESC) = 1
)
SELECT
  submission_date,
  country,
  form_factor,
  advertiser,
  partial,
  position,
  experiments,
  COUNT(*) AS impression_count,
  COUNTIF(is_clicked) AS click_count,
FROM
  deduped
GROUP BY
  submission_date,
  country,
  form_factor,
  advertiser,
  partial,
  position,
  experiments
