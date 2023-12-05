
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.clients_activation_v1`
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
    `moz-fx-data-shared-prod.firefox_ios_derived.clients_activation_v1`
  WHERE
    `submission_date` = @submission_date
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 1) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 1 rows"
      )
    ),
    NULL
  );

#fail
WITH upstream_clients_count AS (
  SELECT
    COUNT(*)
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 6 DAY)
),
activations_clients_count AS (
  SELECT
    COUNT(*)
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.clients_activation_v1`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    (SELECT * FROM upstream_clients_count) <> (SELECT * FROM activations_clients_count),
    ERROR("Number of client records should match for the same first_seen_date."),
    NULL
  );

#fail
SELECT
  IF(
    DATE_DIFF(submission_date, first_seen_date, DAY) <> 6,
    ERROR(
      "Day difference between values inside first_seen_date and submission_date fields should be 6."
    ),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.clients_activation_v1`
WHERE
  `submission_date` = @submission_date;
