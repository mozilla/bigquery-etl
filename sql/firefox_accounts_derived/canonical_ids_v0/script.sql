/*

Right now, this doesn't reference any real telemetry data.

Rather, it is a proof of concept that demonstrates the feasibility of using
a script to loop over a temporary table to follow a chain of connections
between IDs due to password resets.

In this example, `canonical_ids` is the long-term table of associations and
`input` a new day worth of pings. We clean `input` and then enter a loop
where we reach back one step further in the chain of IDs each time.
We exit the loop when we observe no further change (verified by performing
a checksum over the previous_ecosystem_id column).

Finally, we join our working set back to the long-term `canonical_ids` table
to link with original IDs and filter out IDs that we have already seen.

*/

DECLARE checksum_pre,
checksum_post,
run_away_stop
INT64 DEFAULT 0;

CREATE TEMP TABLE
  canonical_ids
AS
SELECT
  'a1' AS ecosystem_user_id,
  'a1' AS canonical_id,
  DATE '2020-01-01' AS first_seen_date
UNION ALL
SELECT
  'a2' AS ecosystem_user_id,
  'a1' AS canonical_id,
  DATE '2020-01-03' AS first_seen_date
UNION ALL
SELECT
  'a3' AS ecosystem_user_id,
  'a1' AS canonical_id,
  DATE '2020-01-05' AS first_seen_date
UNION ALL
SELECT
  'b1' AS ecosystem_user_id,
  'b1' AS canonical_id,
  DATE '2020-02-01' AS first_seen_date
UNION ALL
SELECT
  'b2' AS ecosystem_user_id,
  'b1' AS canonical_id,
  DATE '2020-03-12' AS first_seen_date;

CREATE TEMP TABLE
  input
AS
SELECT
  'a0' AS ecosystem_user_id,
  CAST(NULL AS string) AS previous_ecosystem_user_id
UNION ALL
SELECT
  'a0' AS ecosystem_user_id,
  'a3' AS previous_ecosystem_user_id
UNION ALL
SELECT
  'a0' AS ecosystem_user_id,
  CAST(NULL AS string) AS previous_ecosystem_user_id
UNION ALL
SELECT
  'b3' AS ecosystem_user_id,
  'b2' AS previous_ecosystem_user_id
UNION ALL
SELECT
  'b3' AS ecosystem_user_id,
  'b2' AS previous_ecosystem_user_id
UNION ALL
SELECT
  'b4' AS ecosystem_user_id,
  'b3' AS previous_ecosystem_user_id
UNION ALL
SELECT
  'c1' AS ecosystem_user_id,
  CAST(NULL AS string) AS previous_ecosystem_user_id;

CREATE TEMP TABLE
  working_set
AS
WITH aggregated AS (
  SELECT
    ecosystem_user_id,
    array_agg(DISTINCT previous_ecosystem_user_id IGNORE NULLS) AS previous_ecosystem_user_ids
  FROM
    input
  GROUP BY
    ecosystem_user_id
)
SELECT
  ecosystem_user_id,
  IF(
    array_length(previous_ecosystem_user_ids) > 1,
    ERROR(FORMAT("Found more than 1 previous ID for %s", ecosystem_user_id)),
    previous_ecosystem_user_ids[safe_offset(0)]
  ) AS previous_ecosystem_user_id
FROM
  aggregated;

LOOP
  SET run_away_stop = IF(
    run_away_stop >= 10,
    ERROR(
      "Did not converge after 10 iterations; there may be an infinite loop or a pathological client"
    ),
    run_away_stop + 1
  );

  SET checksum_pre = checksum_post;

  CREATE OR REPLACE TEMP TABLE working_set
  AS
  SELECT
    newer.ecosystem_user_id,
    coalesce(
      older.previous_ecosystem_user_id,
      newer.previous_ecosystem_user_id
    ) AS previous_ecosystem_user_id
  FROM
    working_set AS newer
  LEFT JOIN
    working_set AS older
  ON
    (newer.previous_ecosystem_user_id = older.ecosystem_user_id);

  SET checksum_post = (
    SELECT
      BIT_XOR(FARM_FINGERPRINT(previous_ecosystem_user_id))
    FROM
      working_set
  );

  IF(checksum_pre = checksum_post)
THEN
  BREAK;
END IF;
END LOOP;

WITH canonical_daily AS (
  SELECT
    ecosystem_user_id,
    COALESCE(previous_ecosystem_user_id, ecosystem_user_id) AS canonical_today
  FROM
    ttt
)
SELECT
  cd.ecosystem_user_id,
  coalesce(ci.canonical_id, cd.canonical_today) AS canonical_id,
  CURRENT_DATE() AS first_seen_date
FROM
  canonical_daily cd
LEFT JOIN
  canonical_ids ci
ON
  (cd.canonical_today = ci.ecosystem_user_id)
LEFT JOIN
  canonical_ids existing
ON
  (cd.ecosystem_user_id = existing.ecosystem_user_id)
WHERE
  existing.ecosystem_user_id IS NULL
ORDER BY
  ecosystem_user_id
