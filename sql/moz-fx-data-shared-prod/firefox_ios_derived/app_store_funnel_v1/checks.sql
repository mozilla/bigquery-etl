WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.app_store_funnel_v1`
  WHERE
    `date` = @submission_date
  GROUP BY
    country
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['country'] to be unique.)"
    ),
    NULL
  );
