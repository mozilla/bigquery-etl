WITH dates AS (
  -- Generate the date range to check for missing data
  SELECT
    date,
    table_name
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2021-05-01', DATE(@submission_date), INTERVAL 1 DAY)) AS date,
    UNNEST(
      ['event_aggregates_v1', 'event_aggregates_spons_tiles_v1', 'event_aggregates_suggest_v1']
    ) AS table_name
  WHERE
    -- Add known missing partitions here --
    -- 2021-12-13 is known to be missing in event_aggregates_v1
    (table_name = "event_aggregates_v1" AND date != DATE("2021-12-13"))
    OR
    -- data for tiles and suggest only available after 2022-10-08
    (table_name = "event_aggregates_spons_tiles_v1" AND date >= DATE("2022-10-08"))
    OR (table_name = "event_aggregates_suggest_v1" AND date >= DATE("2022-10-08"))
),
partition_info AS (
  SELECT
    date,
    dates.table_name
  FROM
    dates
  LEFT JOIN
    (
    -- Based on the information we have about existing partitions
    -- determine which partitions are either missing or have 0 rows.
    -- This is cheaper than querying the table every day to get the row count.
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.contextual_services_derived.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        (table_name = 'event_aggregates_v1' AND total_rows > 0)
        OR (table_name = 'event_aggregates_spons_tiles_v1' AND total_rows > 0)
        OR (table_name = 'event_aggregates_suggest_v1' AND total_rows > 0)
    ) AS p
    ON date = PARSE_DATE('%Y%m%d', p.partition_id)
    AND dates.table_name = p.table_name
  WHERE
    p.total_rows IS NULL
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Partitions with data missing: ',
        (
          SELECT
            ARRAY_TO_STRING(
              ARRAY_AGG(CONCAT(table_name, ": ", FORMAT_DATETIME("%Y-%m-%d", date))),
              ", "
            )
          FROM
            partition_info
        )
      )
    ),
    NULL
  )
FROM
  partition_info
