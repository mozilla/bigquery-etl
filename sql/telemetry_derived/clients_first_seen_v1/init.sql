CREATE TEMPORARY TABLE clients_first_seen_dates
PARTITION BY
  first_seen_date
AS
WITH base AS (
  SELECT
    client_id,
    ARRAY_AGG(submission_date) AS dates_seen,
  FROM
    clients_daily_v6
  WHERE
    submission_date >= DATE('2010-01-01')
  GROUP BY
    client_id
)
SELECT
  client_id,
  IF(ARRAY_LENGTH(dates_seen) > 0, dates_seen[OFFSET(0)], NULL) AS first_seen_date,
  IF(ARRAY_LENGTH(dates_seen) > 1, dates_seen[OFFSET(1)], NULL) AS second_seen_date,
FROM
  base;

CREATE TABLE IF NOT EXISTS
  clients_first_seen_v1
PARTITION BY
  first_seen_date
CLUSTER BY
  normalized_channel,
  sample_id
AS
SELECT
  cfsd.first_seen_date,
  cfsd.second_seen_date,
  cd.* EXCEPT (submission_date)
FROM
  clients_daily_v6 AS cd
LEFT JOIN
  clients_first_seen_dates AS cfsd
ON
  (cd.submission_date = cfsd.first_seen_date AND cd.client_id = cfsd.client_id)
WHERE
  cfsd.client_id IS NOT NULL
  AND cd.submission_date >= DATE('2010-01-01')
