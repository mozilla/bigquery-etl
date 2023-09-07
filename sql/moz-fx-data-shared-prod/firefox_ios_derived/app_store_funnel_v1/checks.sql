WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.app_store_funnel_v1`
  GROUP BY
    `date`,
    country
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['`date`', 'country'] to be unique.)"
    ),
    NULL
  );

WITH min_rows AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.app_store_funnel_v1`
  WHERE
    `date` = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1) FROM min_rows) > 0,
    ERROR(
      CONCAT("Less than ", (SELECT total_rows FROM min_rows), " rows found (expected more than 1)")
    ),
    NULL
  );
