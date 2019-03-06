WITH
  current_sample AS (
  SELECT
    -- Include a dummy last_date struct to make schema match with `previous`.
    STRUCT(DATE('2000-01-01'), DATE('2000-01-01')) AS last_date,
    * EXCEPT (submission_date,
      generated_time)
  FROM
    core_clients_daily_v1
  WHERE
    submission_date = @submission_date ),
  previous AS (
  SELECT
    * EXCEPT (submission_date,
      generated_time)
  FROM
    core_clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND last_date.seen > DATE_SUB(@submission_date, INTERVAL 28 DAY) )
SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME() AS generated_time,
  -- This last_date struct is used to record the last date that a particular
  -- client met various usage criteria.
  STRUCT (
    -- last_date.seen is the last date we recieved any core ping at all.
    IF(current_sample.client_id IS NOT NULL,
      @submission_date,
      previous.last_date.seen) AS seen,
    -- last_date.seen_in_tier1_country allows us to calculate a more inclusive
    -- variant of country-segmented MAU where we can still count a client that
    -- appeared in one of the target countries in the previous 28 days even if
    -- the most recent "country" value is not in this set.
    IF(current_sample.client_id IS NOT NULL
      AND current_sample.country IN ('US', 'FR', 'DE', 'UK', 'CA'),
      @submission_date,
      IF(previous.last_date.seen_in_tier1_country > DATE_SUB(@submission_date, INTERVAL 28 DAY),
        previous.last_date.seen_in_tier1_country,
        NULL)) AS seen_in_tier1_country ) AS last_date,
  IF(current_sample.client_id IS NOT NULL,
    current_sample,
    previous).* EXCEPT (last_date)
FROM
  current_sample
FULL JOIN
  previous
USING
  (client_id)
