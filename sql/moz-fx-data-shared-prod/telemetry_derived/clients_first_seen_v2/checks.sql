
#fail
WITH non_unique AS (
  SELECT
    COUNT(*) AS total_count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2`
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
ASSERT(
  SELECT
    COUNTIF(first_seen.client_id IS NULL)
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6` AS daily
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2` AS first_seen
    USING (client_id)
  WHERE
    submission_date = @submission_date
) = 0;

#fail
ASSERT(
  SELECT
    COUNTIF(first_seen.client_id IS NULL)
  FROM
    `moz-fx-data-shared-prod.telemetry.new_profile` AS new_profile
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v2` AS first_seen
    USING (client_id)
  WHERE
    DATE(submission_timestamp) = @submission_date
) = 0;

-- TODO: from https://mozilla-hub.atlassian.net/browse/DS-3102:
-- ratio of new profiles reporting NPP, FSP, MP as the first ping (we if this ratio diverges wildly,
-- we’d want to know) what's the baseline here?
