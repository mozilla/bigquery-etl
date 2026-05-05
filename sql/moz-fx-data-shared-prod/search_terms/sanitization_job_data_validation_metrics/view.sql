CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms.sanitization_job_data_validation_metrics`
AS
SELECT
  finished_at,
  SAFE_DIVIDE(
    total_search_terms_removed_by_sanitization_job,
    total_search_terms_analyzed
  ) AS pct_sanitized_search_terms,
  SAFE_DIVIDE(contained_at, total_search_terms_analyzed) AS pct_sanitized_contained_at,
  SAFE_DIVIDE(contained_numbers, total_search_terms_analyzed) AS pct_sanitized_contained_numbers,
  SAFE_DIVIDE(contained_name, total_search_terms_analyzed) AS pct_sanitized_contained_name,
  SAFE_DIVIDE(
    sum_terms_containing_us_census_surname,
    total_search_terms_analyzed
  ) AS pct_terms_containing_us_census_surname,
  SAFE_DIVIDE(
    sum_uppercase_chars_all_search_terms,
    sum_chars_all_search_terms
  ) AS pct_uppercase_chars_all_search_terms,
  SAFE_DIVIDE(
    sum_words_all_search_terms,
    total_search_terms_analyzed
  ) AS avg_words_all_search_terms,
  1 - SAFE_DIVIDE(languages.english_count, languages.all_languages_count) AS pct_terms_non_english
FROM
  `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata_v2` AS metadata
JOIN
  (
    SELECT
      job_start_time,
      MAX(CASE WHEN language_code = 'en' THEN search_term_count END) english_count,
      SUM(search_term_count) AS all_languages_count,
    FROM
      `moz-fx-data-shared-prod.search_terms.sanitization_job_languages`
    GROUP BY
      job_start_time
  ) AS languages
  ON metadata.started_at = languages.job_start_time
WHERE
  status = 'SUCCESS'
ORDER BY
  finished_at DESC;
