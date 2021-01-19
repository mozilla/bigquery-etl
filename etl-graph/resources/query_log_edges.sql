CREATE TEMP FUNCTION qualified_name(
  meta STRUCT<project_id string, dataset_id string, table_id string>
)
RETURNS string AS (
  CONCAT(meta.project_id, ":", meta.dataset_id, ".", meta.table_id)
);

CREATE TEMP FUNCTION strip_suffix(name string)
RETURNS string AS (
    -- Get rid of the date partition if it exists in the table name, and then extract everything up to the version part.
    -- If the regex fails, just return the name without the partition.
  coalesce(
    REGEXP_EXTRACT(SPLIT(name, "$")[OFFSET(0)], r"^(.*:.*\..*_v[0-9]+)"),
    SPLIT(name, "$")[OFFSET(0)]
  )
);

  -- NOTE: this will capture all links between tables in history. If the queries change over time, then this
  -- may misrepresent the dependencies. One way to solve this is to take the most recent job_id for a destination table
  -- and *then* explode.
WITH extracted AS (
  SELECT
    user_email,
    job_id,
    creation_time,
    destination_table,
    referenced_tables,
    query
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE
    creation_time > TIMESTAMP_SUB(current_timestamp, INTERVAL 90 day)
    AND user_email LIKE "%gserviceaccount.com"
    AND error_result IS NULL
    AND state = "DONE"
    -- dont care about destination tables without references at the moment
    AND referenced_tables IS NOT NULL
    AND NOT STARTS_WITH(destination_table.dataset_id, "_")
),
transformed AS (
  SELECT
    * EXCEPT (destination_table, referenced_tables, project_id, dataset_id, table_id),
    strip_suffix(qualified_name(destination_table)) AS destination_table,
    strip_suffix(qualified_name(referenced_table)) AS referenced_table,
  FROM
    extracted,
    extracted.referenced_tables AS referenced_table
  WHERE
    NOT STARTS_WITH(referenced_table.dataset_id, "_")
),
creation_time_diff AS (
  SELECT
    DISTINCT destination_table,
    creation_time,
    LAG(creation_time) OVER (
      PARTITION BY
        destination_table
      ORDER BY
        creation_time
    ) AS prev_creation_time
  FROM
    transformed
),
creation_time_freq AS (
  SELECT
    destination_table,
    ROUND(AVG(TIMESTAMP_DIFF(creation_time, prev_creation_time, minute)) / 60, 1) AS freq_avg,
    ROUND(stddev(TIMESTAMP_DIFF(creation_time, prev_creation_time, minute)) / 60, 1) AS freq_std
  FROM
    creation_time_diff
  GROUP BY
    1
),
edges AS (
  SELECT
    destination_table,
    referenced_table,
    MAX(creation_time) AS creation_timestamp,
    CAST(COUNT(DISTINCT job_id) AS FLOAT64) AS n_jobs,
  FROM
    transformed
  GROUP BY
    1,
    2
)
SELECT
  *
FROM
  edges
JOIN
  creation_time_freq
USING
  (destination_table)
