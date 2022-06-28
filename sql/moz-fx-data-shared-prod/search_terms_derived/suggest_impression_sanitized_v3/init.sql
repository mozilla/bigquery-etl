CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v3`(
    request_id STRING,
    submission_timestamp TIMESTAMP,
    telemetry_query STRING,
    advertiser STRING,
    block_id INTEGER,
    context_id STRING,
    sample_id INTEGER,
    is_clicked BOOLEAN,
    locale STRING,
    country STRING,
    region STRING,
    normalized_os STRING,
    normalized_os_version STRING,
    normalized_channel STRING,
    position INTEGER,
    reporting_url STRING,
    scenario STRING,
    version STRING,
    timestamp TIMESTAMP,
    query STRING,
    dma STRING,
    form_factor STRING,
    browser STRING,
    os_family STRING,
  )
PARTITION BY
  DATE(submission_timestamp)
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 15);
