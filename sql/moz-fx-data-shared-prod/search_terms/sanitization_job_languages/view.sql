CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms.sanitization_job_languages`
AS
SELECT
  started_at as job_start_time,
  lang.key AS language_code,
  SAFE_CAST(lang.value AS int) AS search_term_count
FROM
  `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata`
CROSS JOIN
  UNNEST(mozfun.json.js_extract_string_map(approximate_language_proportions_json)) AS lang
