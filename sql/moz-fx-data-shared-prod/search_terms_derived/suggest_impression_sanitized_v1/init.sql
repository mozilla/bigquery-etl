CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v1`
PARTITION BY
  DATE(submission_timestamp)
CLUSTER BY
  normalized_channel,
  sample_id
OPTIONS
  (require_partition_filter = TRUE, partition_expiration_days = 15)
AS
SELECT
  * REPLACE (
    (
      SELECT AS STRUCT
        metadata.* REPLACE (
          -- We null out metadata.geo.city in the sanitized data to reduce the possibility
          -- of correlating low-frequency queries with a particular client based on geo.
          (SELECT AS STRUCT metadata.geo.* REPLACE (CAST(NULL AS STRING) AS city)) AS geo
        )
    ) AS metadata
  )
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
WHERE
  DATE(submission_timestamp) >= (CURRENT_DATE() - 14)
