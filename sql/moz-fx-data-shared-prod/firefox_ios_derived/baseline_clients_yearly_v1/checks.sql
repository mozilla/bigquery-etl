
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.baseline_clients_yearly_v1`
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
