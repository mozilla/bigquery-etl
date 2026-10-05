WITH all_queries AS (
  SELECT DISTINCT
    DATE(`timestamp`) AS submission_date,
    request_id,
    query,
    country,
    form_factor
  FROM
    `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3`
  WHERE
    DATE(`timestamp`) = @submission_date
)
SELECT
  submission_date,
  query,
  country,
  form_factor,
  COUNT(*) AS query_count
FROM
  all_queries
GROUP BY
  1,
  2,
  3,
  4
