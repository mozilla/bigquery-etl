
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
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
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Less than ",
        (SELECT total_rows FROM min_row_count),
        " rows found (expected more than 1)"
      )
    ),
    NULL
  );

#fail
-- Here we're checking that the retention_week_2 generated inside funnel_retention_week_2_v1
-- matches that reported by this table (generated 2 weeks later).
WITH retention_week_2 AS (
  SELECT
    COUNTIF(retained_week_2)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_2_v1`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),
retention_week_2_week_4_generated AS (
  SELECT
    COUNTIF(retained_week_2)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.funnel_retention_week_4_v1`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
)
SELECT
  IF(
    (SELECT * FROM retention_week_2) <> (SELECT * FROM retention_week_2_week_4_generated),
    ERROR(
      CONCAT(
        "Retention reported for week 2 by week_2 (",
        (SELECT * FROM retention_week_2),
        ") and week_4 (",
        (SELECT * FROM retention_week_2_week_4_generated),
        ") tables does not match."
      )
    ),
    NULL
  )
