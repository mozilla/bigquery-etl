
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.gclid_conversions_v1`
  GROUP BY
    gclid,
    activity_date
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['gclid', 'activity_date'] to be unique.)"
    ),
    NULL
  );
