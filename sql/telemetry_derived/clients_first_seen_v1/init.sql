CREATE TEMPORARY TABLE client_id_dates_seen
PARTITION BY
  submission_date
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
  cfs.first_seen_date,
  cfs.second_seen_date,
  cd.* EXCEPT (submission_date)
FROM
  clients_daily_v6 AS cd
LEFT JOIN
  client_id_first_seen AS cfs
ON
  (cd.submission_date = cfs.first_seen_date AND cd.client_id = cfs.client_id)
WHERE
  cfs.client_id IS NOT NULL
  AND cd.submission_date >= DATE('2010-01-01')
