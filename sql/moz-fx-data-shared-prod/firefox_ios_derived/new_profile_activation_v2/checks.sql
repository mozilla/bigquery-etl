
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.firefox_ios_derived.new_profile_activation_v2`
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
    `moz-fx-data-shared-prod.firefox_ios_derived.new_profile_activation_v2`
  WHERE
    `date` = @submission_date
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
SELECT
  IF(
    COUNTIF(is_new_profile) <> COUNT(*),
    ERROR("Number of is_new_profile TRUE values should be the same as the row count."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.new_profile_activation_v2`
WHERE
  `date` = @submission_date;

#fail
SELECT
  IF(
    DATE_DIFF(`date`, first_seen_date, DAY) <> 6,
    ERROR("Day difference between values inside `date` and submission_date fields should be 6."),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.new_profile_activation_v2`
WHERE
  `date` = @submission_date;
