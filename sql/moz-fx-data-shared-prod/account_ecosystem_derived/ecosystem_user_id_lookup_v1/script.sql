DECLARE checksum_pre,
checksum_post,
run_away_stop INT64 DEFAULT 0;

-- We initialize our "working set" temp table as the distinct set of client
-- observations on the target date.
CREATE TEMP TABLE
  working_set
AS
WITH unioned AS (
  SELECT
    submission_timestamp,
    ecosystem_user_id,
    previous_ecosystem_user_ids[SAFE_OFFSET(0)] AS previous_ecosystem_user_id
  FROM
    firefox_accounts_stable.account_ecosystem_v1
  UNION ALL
  SELECT
    submission_timestamp,
    payload.ecosystem_user_id,
    payload.previous_ecosystem_user_ids[SAFE_OFFSET(0)] AS previous_ecosystem_user_id
  FROM
    telemetry_stable.account_ecosystem_v4
),
aggregated AS (
  SELECT
    ecosystem_user_id,
    ARRAY_AGG(DISTINCT previous_ecosystem_user_id IGNORE NULLS) AS previous_ecosystem_user_ids
  FROM
    unioned
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    ecosystem_user_id
)
SELECT
  ecosystem_user_id,
  IF(
    ARRAY_LENGTH(previous_ecosystem_user_ids) > 1,
    ERROR(FORMAT("Found more than 1 previous ID for %s", ecosystem_user_id)),
    previous_ecosystem_user_ids[SAFE_OFFSET(0)]
  ) AS previous_ecosystem_user_id
FROM
  aggregated;

-- If a user resets their FxA password multiple times in a single day, they
-- will receive several new ecosystem_user_id values, each time emitting an
-- event with the association between the new ID and the old. We need to be
-- able to follow this chain of events to associate each of these IDs back
-- to the earliest one.
-- To accommodate this, we enter a loop where on each iteration we rewrite
-- the working_set, linking back one step in the chain of IDs. We know we're
-- done when the working_set remains unchanged after an iteration.
LOOP
  SET run_away_stop = IF(
    run_away_stop >= 50,
    ERROR(
      "Did not converge after 50 iterations; there may be an infinite loop or a pathological client"
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
      IFNULL(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(working_set))), 0)
    FROM
      working_set
  );

  IF(checksum_pre = checksum_post)
THEN
  BREAK;
END IF;
END LOOP;

-- We now insert into the target table, including only newly observed
-- ecosystem_user_id values.
INSERT INTO
  ecosystem_user_id_lookup_v1(ecosystem_user_id, canonical_id, first_seen_date)
WITH canonical_daily AS (
  SELECT
    ecosystem_user_id,
    COALESCE(previous_ecosystem_user_id, ecosystem_user_id) AS canonical_today
  FROM
    working_set
)
SELECT
  cd.ecosystem_user_id,
  coalesce(ci.canonical_id, cd.canonical_today) AS canonical_id,
  @submission_date AS first_seen_date
FROM
  canonical_daily cd
LEFT JOIN
  ecosystem_user_id_lookup_v1 AS ci
ON
  (cd.canonical_today = ci.ecosystem_user_id)
LEFT JOIN
  ecosystem_user_id_lookup_v1 AS existing
ON
  (cd.ecosystem_user_id = existing.ecosystem_user_id)
WHERE
  existing.ecosystem_user_id IS NULL;
