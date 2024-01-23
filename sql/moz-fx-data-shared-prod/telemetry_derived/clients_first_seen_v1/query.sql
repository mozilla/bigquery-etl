WITH today AS (
  SELECT
    CAST(NULL AS DATE) AS first_seen_date,
    CAST(NULL AS DATE) AS second_seen_date,
    * EXCEPT (submission_date)
  FROM
    clients_daily_v6
  WHERE
    submission_date = @submission_date
),
previous AS (
  -- If we need to reprocess data, we have to make sure to delete all data
  -- earlier than the passed @submission_date parameter, so we null out
  -- invalid second_seen_date values and drop rows invalid first_seen_date.
  SELECT
    * REPLACE (IF(second_seen_date >= @submission_date, NULL, second_seen_date) AS second_seen_date)
  FROM
    clients_first_seen_v1
  WHERE
    first_seen_date < @submission_date
)
SELECT
  -- Only insert dimensions from clients_daily if this is the first time the
  -- client has been seen; otherwise, we copy over the existing dimensions
  -- from the first sighting.
  IF(previous.client_id IS NULL, today, previous).* REPLACE (
    IF(
      previous.first_seen_date IS NULL,
      @submission_date,
      previous.first_seen_date
    ) AS first_seen_date,
    IF(
      previous.second_seen_date IS NULL
      AND previous.first_seen_date IS NOT NULL
      AND today.client_id IS NOT NULL,
      @submission_date,
      previous.second_seen_date
    ) AS second_seen_date
  )
FROM
  previous
FULL JOIN
  today
  USING (client_id)
