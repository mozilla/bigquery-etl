CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.sanitized_search_terms_v1`
PARTITION BY
  DATE(timestamp)
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 15)
AS
SELECT
  *
FROM
  `mozdata.search_terms_unsanitized_analysis.prototype_sanitized_data`
WHERE
  DATE(timestamp) >= (CURRENT_DATE() - 14)
