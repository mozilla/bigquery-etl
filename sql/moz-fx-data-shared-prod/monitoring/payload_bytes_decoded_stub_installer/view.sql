CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_stub_installer`
AS
SELECT
  * EXCEPT (payload),
  SPLIT(_TABLE_SUFFIX, '__')[OFFSET(0)] AS namespace,
  SPLIT(_TABLE_SUFFIX, '__')[OFFSET(1)] AS doc_type,
  REGEXP_EXTRACT(_TABLE_SUFFIX, r"^.*_v(.*)$") AS doc_version
FROM
  `moz-fx-data-shared-prod.payload_bytes_decoded.stub_installer_*`
