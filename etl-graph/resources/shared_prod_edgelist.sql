CREATE TEMP FUNCTION qualified_name(
  meta STRUCT<project_id string, dataset_id string, table_id string>
)
RETURNS string AS (
  CONCAT(meta.project_id, ":", meta.dataset_id, ".", meta.table_id)
);

CREATE TEMP FUNCTION strip_suffix(name string)
RETURNS string AS (
    -- case: moz-fx-data-shared-prod:tmp.active_profiles_v1_2020_04_27_6e09b8
    -- case: moz-fx-data-shared-prod:telemetry_derived.attitudes_daily_v1$20200314
    -- case: moz-fx-data-shared-prod:telemetry_derived.attitudes_daily$20200314
    -- Get rid of the date partition if it exists, and then extract everything up to the version part.
    -- If the regex fails, just return the name without the partition.
  coalesce(
    REGEXP_EXTRACT(SPLIT(name, "$")[OFFSET(0)], r"^(.*_v[0-9]+)"),
    SPLIT(name, "$")[OFFSET(0)]
  )
);

  -- NOTE: this will capture all links between tables in history. If the queries change over time, then this
  -- may misrepresent the dependencies. One way to solve this is to take the most recent job_id for a destination table
  -- and *then* explode.
WITH extracted AS (
  SELECT
    creation_time,
    destination_table,
    referenced_tables,
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE
    error_result IS NULL
    AND state = "DONE"
    -- dont care about destination tables without references at the moment
    AND referenced_tables IS NOT NULL
    AND NOT STARTS_WITH(destination_table.dataset_id, "_")
),
transformed AS (
  SELECT
    creation_time,
    strip_suffix(qualified_name(destination_table)) AS destination_table,
    strip_suffix(qualified_name(referenced_table)) AS referenced_table,
  FROM
    extracted,
    extracted.referenced_tables AS referenced_table
  WHERE
    NOT STARTS_WITH(referenced_table.dataset_id, "_")
)
-- TODO: include edge weight by counting the number of times it has been referenced
SELECT
  creation_time,
  destination_table,
  referenced_table,
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          destination_table,
          referenced_table
        ORDER BY
          creation_time
      ) AS _rank
    FROM
      transformed
  )
WHERE
  _rank = 1
  -- known deprecated table filtered here, though this would get filtered when joined against the set of known tables
  AND destination_table NOT LIKE "%bgbb_clients_day%"
ORDER BY
  destination_table,
  creation_time
