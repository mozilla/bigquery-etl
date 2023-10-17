
-- Generated via bigquery_etl.glean_usage
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.pine_derived.baseline_clients_first_seen_v1`
  GROUP BY
    client_id
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['client_id'] to be unique.)"
    ),
    NULL
  );
