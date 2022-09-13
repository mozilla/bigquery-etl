CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.unsanitized_search_term_sample_data`(
    timestamp timestamp,
    request_id string,
    session_id string,
    sequence_no integer,
    query string,
    country string,
    region string,
    dma string,
    form_factor string,
    browser string,
    os_family string
  )
PARTITION BY
  DATE(timestamp)
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 15);
