CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.data_governance_metadata.column_classifications`
AS
SELECT
  * EXCEPT (evidence),
  (SELECT AS STRUCT evidence.* EXCEPT (sanitized_samples)) AS evidence,
FROM
  `moz-fx-data-shared-prod.data_governance_metadata_derived.column_classifications_v1`
