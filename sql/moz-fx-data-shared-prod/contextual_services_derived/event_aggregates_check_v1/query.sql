WITH dates AS (
  -- Generate the date range to check for missing data
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2021-05-01', '@submission_date', INTERVAL 1 DAY)) AS date
),
partition_info AS (
  SELECT
    date
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
        `contextual_services_derived.INFORMATION_SCHEMA.PARTITIONS`
      WHERE
        table_name = 'event_aggregates_v1'
        AND total_rows > 0
    ) AS p
  ON
    date = PARSE_DATE('%Y%m%d', p.partition_id)
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
            ARRAY_TO_STRING(ARRAY_AGG(FORMAT_DATETIME("%Y-%m-%d", date)), ", ")
          FROM
            partition_info
        )
      )
    ),
    NULL
  )
FROM
  partition_info
