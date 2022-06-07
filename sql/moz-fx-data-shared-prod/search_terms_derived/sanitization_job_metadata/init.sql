CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata`(
    status string,
    started_at timestamp,
    finished_at timestamp,
    sanitized_contained_at integer,
    sanitized_contained_numbers integer,
    sanitized_contained_name integer,
    total_sanitized_search_terms integer,
    total_search_terms_analyzed integer,
    sum_chars_all_search_terms integer,
    sum_words_all_search_terms integer,
    sum_uppercase_chars_all_search_terms integer,
    sum_terms_containing_us_census_surname integer,
    approximate_language_proportions_json string,
    failure_reason string
  );
