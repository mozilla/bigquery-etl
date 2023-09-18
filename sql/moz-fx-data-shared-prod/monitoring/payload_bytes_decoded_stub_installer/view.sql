CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_stub_installer`
AS
SELECT
  'stub_installer' AS pipeline_family,
  * EXCEPT (payload) REPLACE(
    -- We normalize the order of metadata fields to be consistent across
    -- pipeline families, allowing UNION ALL queries.
    STRUCT(
      metadata.document_namespace,
      metadata.document_type,
      metadata.document_version,
      metadata.geo,
      metadata.header,
      metadata.isp,
      metadata.uri
    ) AS metadata
  )
FROM
  `moz-fx-data-shared-prod.payload_bytes_decoded.stub_installer*`
