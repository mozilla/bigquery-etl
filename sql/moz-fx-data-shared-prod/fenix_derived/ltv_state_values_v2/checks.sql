
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.fenix_derived.ltv_state_values_v2`
  GROUP BY
    country,
    state
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['country', 'state'] to be unique.)"
    ),
    NULL
  );

#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.fenix_derived.ltv_state_values_v2`
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1000) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 1000 rows"
      )
    ),
    NULL
  );

#fail
-- Each country should have a single state function
SELECT
  mozfun.assert.equals(1, COUNT(DISTINCT state_function))
FROM
  fenix_derived.ltv_state_values_v1
GROUP BY
  country;

#fail
-- There should be more than 2 countries present
SELECT
  `mozfun.assert.true`(COUNT(DISTINCT country) > 2)
FROM
  fenix_derived.ltv_state_values_v1;
