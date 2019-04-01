CREATE IF NOT EXISTS TABLE
  clients_monthly_v1
PARTITION BY
  submission_date AS
SELECT
  DATE(NULL) AS submission_date,
  CURRENT_DATETIME AS generated_time,
  client_id,
  sample_id,
  ARRAY(
  SELECT
    AS STRUCT clients_daily_v6.* EXCEPT (client_id,
      sample_id)) AS days
FROM
  clients_daily_v6
WHERE
  -- Output empty table and read no input rows
  FALSE
