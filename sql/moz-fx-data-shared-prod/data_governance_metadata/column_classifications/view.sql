-- Everything in column_classifications_v1 except evidence.sanitized_samples.
-- Columns are listed rather than wildcarded so that a new column in the
-- restricted table is not exposed to mozilla-confidential until someone adds
-- it here on purpose.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.data_governance_metadata.column_classifications`
AS
SELECT
  classified_at,
  run_id,
  source_project,
  source_dataset,
  source_table,
  column_name,
  data_type,
  primary_label,
  secondary_labels,
  confidence,
  reasoning,
  needs_review,
  matched_probe,
  data_sensitivity,
  model,
  taxonomy_version,
  input_hash,
  IF(
    evidence IS NULL,
    NULL,
    STRUCT(
      evidence.null_rate,
      evidence.distinct_count,
      evidence.is_high_cardinality,
      evidence.column_tier,
      evidence.existing_description
    )
  ) AS evidence,
FROM
  `moz-fx-data-shared-prod.data_governance_metadata_derived.column_classifications_v1`
