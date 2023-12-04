
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

#fail
-- Should have a partition with rows for each date
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.baseline_clients_yearly_v1`
  WHERE
    submission_date = @submission_date
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

#fail
-- Should have clients who were active on that date present
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.baseline_clients_yearly_v1`
  WHERE
    `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bytes) = 0
    AND submission_date = @submission_date
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

#fail
-- Should have a bunch of new profiles each date
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.baseline_clients_yearly_v1`
  WHERE
    first_seen_date = @submission_date
    AND submission_date = @submission_date
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
