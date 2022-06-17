CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3`(
    timestamp timestamp,
    request_id string,
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
