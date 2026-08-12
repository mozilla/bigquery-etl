CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v4`
AS
SELECT
  -- Cloud Logging sinks write ingestion-time partitioned tables, so callers must
  -- filter on this to prune partitions; `timestamp` alone scans the whole table.
  _PARTITIONDATE AS partition_date,
  `timestamp`,
  jsonPayload.fields.rid AS request_id,
  jsonPayload.fields.query AS query,
  -- Merino emits the string 'none' rather than NULL for missing geo fields.
  NULLIF(CAST(jsonPayload.fields.country AS STRING), 'none') AS country,
  NULLIF(CAST(jsonPayload.fields.region AS STRING), 'none') AS region,
  NULLIF(CAST(jsonPayload.fields.dma AS STRING), 'none') AS dma,
  -- city is deliberately not propagated, matching merino_log_sanitized_v3.
  jsonPayload.fields.form_factor AS form_factor,
  jsonPayload.fields.browser AS browser,
  jsonPayload.fields.os_family AS os_family,
  jsonPayload.fields.session_id AS session_id,
  CAST(jsonPayload.fields.sequence_no AS INT64) AS sequence_no,
FROM
  `moz-fx-merino-prod-5de4.search_terms.stdout`
