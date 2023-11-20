
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_clients_v1`
  GROUP BY
    ga_client_id
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['ga_client_id'] to be unique.)"
    ),
    NULL
  );

#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_clients_v1`
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1000000) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 1000000 rows"
      )
    ),
    NULL
  );
