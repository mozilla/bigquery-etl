-- Checks generated using a template for shredder mitigation.
-- Rows in previous version not matching in new version. Mismatches can happen when the row is
-- missing or any column doesn't match, including a single metric difference.

#fail
WITH {{ previous_version_cte | default('previous_version') }} AS (
  {{ previous_version | default('SELECT 1')  }}
),
{{ new_version_cte | default('new_version') }} AS (
  {{ new_version | default('SELECT 1')  }}
),
previous_not_matching AS (
  SELECT
    *
  FROM
    {{ previous_version_cte | default('previous_version') }}
  EXCEPT DISTINCT
  SELECT
    *
  FROM
    {{ new_version_cte | default('new_version') }}
)
SELECT
  IF(
    (SELECT COUNT(*) FROM previous_not_matching) > 0,
    ERROR(
      CONCAT(
        ((SELECT COUNT(*) FROM previous_not_matching)),
        " rows in the previous data don't match backfilled data! Run auto-generated checks for ",
        "all mismatches & search for rows missing or with differences in metrics. Sample row in previous version: ",
        (SELECT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM previous_not_matching LIMIT 1)))
      )
    ),
    NULL
  );

-- Rows in new version not matching in previous version. It could be rows added by the process or rows with differences.

#fail
WITH {{ previous_version_cte | default('previous_version') }} AS (
  {{ previous_version | default('SELECT 1')  }}
),
{{ new_version_cte | default('new_version') }} AS (
  {{ new_version | default('SELECT 1')  }}
),
backfilled_not_matching AS (
  SELECT
    *
  FROM
    {{ new_version_cte | default('new_version') }}
  EXCEPT DISTINCT
  SELECT
    *
  FROM
    {{ previous_version_cte | default('previous_version') }}
)
SELECT
  IF(
    (SELECT COUNT(*) FROM backfilled_not_matching) > 0,
    ERROR(
      CONCAT(
        ((SELECT COUNT(*) FROM backfilled_not_matching)),
        " rows in backfill don't match previous version of data! Run auto-generated checks for ",
        "all mismatches & search for rows added or with differences in metrics. Sample row in new_version: ",
        (SELECT TO_JSON_STRING(ARRAY(SELECT AS STRUCT * FROM backfilled_not_matching LIMIT 1)))
      )
    ),
    NULL
  );
