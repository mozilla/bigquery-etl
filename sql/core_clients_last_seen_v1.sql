WITH
  current_sample AS (
  SELECT
    -- Include dummy dates for date_last_* fields to make schema match with `previous`.
    DATE '2000-01-01' AS date_last_seen,
    DATE '2000-01-01' AS date_last_seen_in_tier1_country,
    * EXCEPT (submission_date)
  FROM
    core_clients_daily_v1
  WHERE
    submission_date = @submission_date ),
  previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    core_clients_last_seen_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND date_last_seen > DATE_SUB(@submission_date, INTERVAL 28 DAY) )
SELECT
  @submission_date AS submission_date,
  -- Record the last day on which we recieved any core ping at all from this client.
  IF(current_sample.client_id IS NOT NULL,
    @submission_date,
    previous.date_last_seen) AS date_last_seen,
  -- Record the last day on which the client was in a "Tier 1" country;
  -- this allows a variant of country-segmented MAU where we can still count
  -- a client that appeared in one of the target countries in the previous
  -- 28 days even if the most recent "country" value is not in this set.
  IF(current_sample.client_id IS NOT NULL
    AND current_sample.country IN ('US', 'FR', 'DE', 'GB', 'CA'),
    @submission_date,
    IF(previous.date_last_seen_in_tier1_country > DATE_SUB(@submission_date, INTERVAL 28 DAY),
      previous.date_last_seen_in_tier1_country,
      NULL)) AS date_last_seen_in_tier1_country,
  IF(current_sample.client_id IS NOT NULL,
    current_sample,
    previous).* EXCEPT (date_last_seen, date_last_seen_in_tier1_country)
FROM
  current_sample
FULL JOIN
  previous
USING
  (client_id)
