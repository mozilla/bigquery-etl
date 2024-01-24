
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.fenix_derived.client_ltv_v1`
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

#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.fenix_derived.client_ltv_v1`
  WHERE
    as_of_date = @submission_date
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 10000) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 10000 rows"
      )
    ),
    NULL
  );
