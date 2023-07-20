CREATE OR REPLACE TABLE
  fenix_derived.clients_yearly_v1
PARTITION BY
  (submission_date)
CLUSTER BY
  (sample_id)
OPTIONS
  (require_partition_filter = TRUE)
AS
WITH base AS (
  -- There are duplicates now in `baseline_clients_daily` because of the join with `clients_first_seen`,
  -- so we take the minimum
  SELECT
    * EXCEPT (normalized_app_id),
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        first_seen_date ASC,
        first_run_date ASC
    ) AS rn,
  FROM
    fenix.baseline_clients_daily
  WHERE
    submission_date >= "2021-08-01"
  QUALIFY
    rn = 1
)
SELECT
  -- In this raw table, we capture the history of activity over the past
  -- 365 days for each usage criterion as an array of bytes. The
  -- rightmost bit represents whether the user was active in the current day.
  DATE_ADD(submission_date, INTERVAL off DAY) AS submission_date,
  client_id,
  -- bits_from_offsets does not return a consistent number of bytes, and BQ requires that these match
  -- So we expand out the bit pattern using LPAD, then take the last 365 bits
  mozfun.bytes.extract_bits(
    LPAD(udf.bits_from_offsets(ARRAY_AGG(off)), 46, b'\x00'),
    -365,
    365
  ) AS days_seen_bytes,
  ANY_VALUE(_base HAVING MIN off).* EXCEPT (submission_date, client_id),
FROM
  base AS _base
CROSS JOIN
  UNNEST(GENERATE_ARRAY(0, 364)) AS off
GROUP BY
  client_id,
  submission_date
