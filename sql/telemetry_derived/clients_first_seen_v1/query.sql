WITH today AS (
  SELECT
    NULL AS first_seen_date,
    NULL AS second_seen_date,
    * EXCEPT (submission_date)
  FROM
    clients_daily_v1
  WHERE
    submission_date = @submission_date
)
SELECT
  IFNULL(cfs.first_seen_date, @submission_date) AS first_seen_date,
  IFNULL(
    cfs.second_seen_date,
    IF(cfs.first_seen_date IS NULL, NULL, @submission_date)
  ) AS second_seen_date,
  IF(cfs.client_id IS NULL, today, cfs).* EXCEPT (first_seen_date, second_seen_date)
FROM
  clients_first_seen_v1 AS cfs
FULL JOIN
  today
USING
  (client_id)
WHERE
  cd.submission_date = @submission_date
