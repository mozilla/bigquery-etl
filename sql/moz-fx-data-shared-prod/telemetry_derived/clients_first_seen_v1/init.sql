DECLARE sample_id_iter INT64 DEFAULT 0;

CREATE OR REPLACE TABLE
  telemetry_derived.clients_first_seen_v1
PARTITION BY
  first_seen_date
CLUSTER BY
  normalized_channel,
  sample_id
AS
SELECT
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS second_seen_date,
  cd.* EXCEPT (submission_date)
FROM
  telemetry_derived.clients_daily_v6 AS cd
WHERE
  FALSE;

/*

We cannot create the entire table in a single query, as it exceeds the
shuffle size limit, presumably because it has to distribute the entire
clients_first_seen_dates table to all nodes to join against.

We instead populate this table in a loop, inserting data for each sample_id
individually. During initial testing with on-demand query slots, each iteration
completes in ~3 minutes and scans ~3 TB of data.

*/
LOOP
  CREATE OR REPLACE TEMPORARY TABLE clients_first_seen_dates
  PARTITION BY
    first_seen_date
  AS
  WITH base AS (
    SELECT
      client_id,
      ARRAY_AGG(submission_date ORDER BY submission_date) AS dates_seen,
    FROM
      telemetry_derived.clients_daily_v6
    WHERE
      submission_date >= DATE('2010-01-01')
      AND sample_id = sample_id_iter
    GROUP BY
      client_id
  )
  SELECT
    client_id,
    IF(ARRAY_LENGTH(dates_seen) > 0, dates_seen[OFFSET(0)], NULL) AS first_seen_date,
    IF(ARRAY_LENGTH(dates_seen) > 1, dates_seen[OFFSET(1)], NULL) AS second_seen_date,
  FROM
    base;

  INSERT
    telemetry_derived.clients_first_seen_v1
  SELECT
    cfsd.first_seen_date,
    cfsd.second_seen_date,
    cd.* EXCEPT (submission_date)
  FROM
    telemetry_derived.clients_daily_v6 AS cd
  LEFT JOIN
    clients_first_seen_dates AS cfsd
    ON (cd.submission_date = cfsd.first_seen_date AND cd.client_id = cfsd.client_id)
  WHERE
    cfsd.client_id IS NOT NULL
    AND cd.submission_date >= DATE('2010-01-01')
    AND sample_id = sample_id_iter;

  SET sample_id_iter = sample_id_iter + 1;

  IF
    sample_id_iter = 100
  THEN
    LEAVE;
  END IF;
END LOOP;
