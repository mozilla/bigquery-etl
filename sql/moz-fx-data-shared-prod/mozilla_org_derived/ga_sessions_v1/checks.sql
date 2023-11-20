
#fail
-- ga_session_id should be unique across all partitions
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v1`
  WHERE
    session_date = @session_date
  GROUP BY
    ga_session_id
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['ga_session_id'] to be unique.)"
    ),
    NULL
  );

#fail
WITH min_row_count AS (
  SELECT
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v1`
  WHERE
    session_date = @session_date
)
SELECT
  IF(
    (SELECT COUNTIF(total_rows < 100) FROM min_row_count) > 0,
    ERROR(
      CONCAT(
        "Min Row Count Error: ",
        (SELECT total_rows FROM min_row_count),
        " rows found, expected more than 100 rows"
      )
    ),
    NULL
  );
