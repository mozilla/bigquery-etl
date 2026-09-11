CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v4`
AS
SELECT
  `timestamp`,
  -- See field request_id in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  jsonPayload.fields.request_id AS request_id,
  -- See field query in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  -- The values of query are sanitized directly in Merino.
  jsonPayload.fields.query AS query,
  -- See fields country, region, and dma in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  -- Merino emits the string 'none' rather than NULL for missing geo fields.
  -- City is deliberately not propagated, matching merino_log_sanitized_v3.
  NULLIF(CAST(jsonPayload.fields.country AS STRING), 'none') AS country,
  NULLIF(CAST(jsonPayload.fields.region AS STRING), 'none') AS region,
  NULLIF(CAST(jsonPayload.fields.dma AS STRING), 'none') AS dma,
  -- See field form_factor in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  jsonPayload.fields.form_factor AS form_factor,
  -- See field browser in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  jsonPayload.fields.browser AS browser,
  -- See field os_family in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  jsonPayload.fields.os_family AS os_family,
  -- See field session_id in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  jsonPayload.fields.session_id AS session_id,
  -- See field sequence_no in sql/moz-fx-data-shared-prod/search_terms_derived/dataset_schema.yaml
  CAST(jsonPayload.fields.sequence_no AS INT64) AS sequence_no,
FROM
  `moz-fx-merino-prod-5de4.search_terms.stdout`
