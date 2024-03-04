
#warn
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v2`
  GROUP BY
    ga_client_id,
    ga_session_id
  HAVING
    total_count > 1
)
SELECT
  IF(
    (SELECT COUNT(*) FROM non_unique) > 0,
    ERROR(
      "Duplicates detected (Expected combined set of values for columns ['ga_client_id', 'ga_session_id'] to be unique.)"
    ),
    NULL
  );

#warn
WITH null_checks AS (
  SELECT
    [
      IF(COUNTIF(session_date IS NULL) > 0, "session_date", NULL),
      IF(COUNTIF(ga_client_id IS NULL) > 0, "ga_client_id", NULL),
      IF(COUNTIF(ga_session_id IS NULL) > 0, "ga_session_id", NULL)
    ] AS checks
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v2`
),
non_null_checks AS (
  SELECT
    ARRAY_AGG(u IGNORE NULLS) AS checks
  FROM
    null_checks,
    UNNEST(checks) AS u
)
SELECT
  IF(
    (SELECT ARRAY_LENGTH(checks) FROM non_null_checks) > 0,
    ERROR(
      CONCAT(
        "Columns with NULL values: ",
        (SELECT ARRAY_TO_STRING(checks, ", ") FROM non_null_checks)
      )
    ),
    NULL
  );
