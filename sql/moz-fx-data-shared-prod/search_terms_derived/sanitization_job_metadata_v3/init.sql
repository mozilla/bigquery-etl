CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata_v3`(
    status string,
    started_at timestamp,
    finished_at timestamp,
    total_search_terms_appearing_in_allow_list integer,
    contained_at integer,
    contained_numbers integer,
    contained_name integer,
    total_search_terms_removed_by_sanitization_job integer,
    total_search_terms_analyzed integer,
    sum_chars_all_search_terms integer,
    sum_words_all_search_terms integer,
    sum_uppercase_chars_all_search_terms integer,
    sum_terms_containing_us_census_surname integer,
    approximate_language_proportions_json string,
    failure_reason string,
    implementation_notes string
  );
