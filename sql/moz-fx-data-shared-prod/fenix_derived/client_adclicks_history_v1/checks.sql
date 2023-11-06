
#fail
-- Grain is: client_id
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.fenix_derived.client_adclicks_history_v1`
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
    `moz-fx-data-shared-prod.fenix_derived.client_adclicks_history_v1`
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 100000) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Less than ",
        (SELECT total_rows FROM min_row_count),
        " rows found (expected more than 100000)"
      )
    ),
    NULL
  );
