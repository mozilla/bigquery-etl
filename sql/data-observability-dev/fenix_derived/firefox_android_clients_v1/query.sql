SELECT
  *,
  -- add some additional fields to test schema changes
  CAST(NULL AS STRING) AS additional_field_1,
  CAST(NULL AS INT64) AS additional_field_2,
  CAST(NULL AS STRING) AS additional_field_3,
  CAST(NULL AS STRING) AS additional_field_4,
FROM
  `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
