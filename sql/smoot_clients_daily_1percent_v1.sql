CREATE TABLE
  smoot_clients_daily_1percent_v1
PARTITION BY
  submission_date_s3
CLUSTER BY
  sample_id,
  client_id AS
SELECT
  *
FROM
  clients_daily_v6
WHERE
  sample_id = "0"
  AND submission_date_s3 >= '2017-12-01'
