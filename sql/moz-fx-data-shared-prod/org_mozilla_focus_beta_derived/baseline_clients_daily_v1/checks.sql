
-- Generated via bigquery_etl.glean_usage
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_derived.baseline_clients_daily_v1`
  WHERE
    submission_date = @submission_date
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
