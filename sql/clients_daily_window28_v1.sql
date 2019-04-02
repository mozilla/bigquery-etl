WITH
  _current AS (
  SELECT
    client_id,
    sample_id,
    ARRAY(
    SELECT
      AS STRUCT clients_daily_v6.* EXCEPT (client_id,
        sample_id)) AS days
  FROM
    clients_daily_v6
  WHERE
    submission_date_s3 = @submission_date ),
  _previous AS (
  SELECT
    client_id,
    sample_id,
    ARRAY(
    SELECT
      day
    FROM
      UNNEST(days) AS day
    WHERE
      submission_date_s3 > DATE_SUB(@submission_date, INTERVAL 28 DAY)) AS days
  FROM
    clients_monthly_v1
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY) )
SELECT
  @submission_date AS submission_date,
  CURRENT_DATETIME AS generated_time,
  client_id,
  sample_id,
  ARRAY_CONCAT(_current.days, previous.days) AS days
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id,
    sample_id)
WHERE
  _current.client_id IS NOT NULL
  OR ARRAY_LENGTH(_previous.days) > 0
