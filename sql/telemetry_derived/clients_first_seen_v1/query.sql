WITH today AS (
  SELECT
    CAST(NULL AS DATE) AS first_seen_date,
    CAST(NULL AS DATE) AS second_seen_date,
    * EXCEPT (submission_date)
  FROM
    clients_daily_v1
  WHERE
    submission_date = @submission_date
)
SELECT
  --
  -- Logic for first_seen_date
  CASE
  WHEN
    cfs.first_seen_date IS NULL
  THEN
    @submission_date
  ELSE
    cfs.first_seen_date
  END
  AS first_seen_date,
  --
  -- Logic for second_seen_date
  CASE
  WHEN
    cfs.first_seen_date IS NULL
  THEN
    NULL
  WHEN
    cfs.second_seen_date IS NULL
  THEN
    @submission_date
  ELSE
    cfs.second_seen_date
  END
  AS second_seen_date,
  --
  -- Only insert dimensions from clients_daily if this is the first time the
  -- client has been seen; otherwise, we copy over the existing dimensions
  -- from the first sighting.
  IF(cfs.client_id IS NULL, today, cfs).* EXCEPT (first_seen_date, second_seen_date)
FROM
  clients_first_seen_v1 AS cfs
FULL JOIN
  today
USING
  (client_id)
WHERE
  cd.submission_date = @submission_date
