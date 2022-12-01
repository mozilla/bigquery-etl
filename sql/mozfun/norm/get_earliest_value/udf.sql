CREATE OR REPLACE FUNCTION norm.get_earliest_value(
  value_set ARRAY<STRUCT<value STRING, value_date DATETIME>>
)
RETURNS STRUCT<earliest_value STRING, earliest_date DATETIME> AS (
  (
    WITH unnested AS (
      SELECT
        *
      FROM
        UNNEST(value_set)
        WITH OFFSET AS offset
      ORDER BY
        offset ASC
    )
    SELECT
      ARRAY_AGG(STRUCT(value, value_date) ORDER BY value_date ASC)[SAFE_OFFSET(0)]
    FROM
      unnested
    WHERE
      value IS NOT NULL
      AND value_date IS NOT NULL
  )
);

-- Tests
SELECT
  assert.equals(
    STRUCT(
      CAST('metrics' AS STRING) AS earliest_value,
      DATETIME('2021-12-12T11:45:00') AS earliest_date
    ),
    norm.get_earliest_value(
      [
        (STRUCT(CAST(NULL AS STRING), DATETIME('2022-01-01 12:00:00'))),
        (STRUCT(CAST('first_session' AS STRING), DATETIME('2022-01-01 11:55:00'))),
        (STRUCT(CAST('metrics' AS STRING), DATETIME('2021-12-12 11:45:00')))
      ]
    )
  ),
  assert.equals(
    STRUCT(
      CAST('existing' AS STRING) AS earliest_value,
      DATETIME('2019-01-01T12:00:00') AS earliest_date
    ),
    norm.get_earliest_value(
      [
        (STRUCT(CAST('existing' AS STRING), DATETIME('2019-01-01 12:00:00'))),
        (STRUCT(CAST('metrics' AS STRING), NULL)),
        (STRUCT(CAST('first_session' AS STRING), DATETIME('2021-12-12 12:00:00')))
      ]
    )
  ),
  assert.equals(
    STRUCT(
      CAST('first_session' AS STRING) AS earliest_value,
      DATETIME('2022-12-12T11:45:00') AS earliest_date
    ),
    norm.get_earliest_value(
      [
        (STRUCT(CAST(NULL AS STRING), DATETIME('2022-12-12 12:00:00'))),
        (STRUCT(CAST('first_session' AS STRING), DATETIME('2022-12-12 11:45:00'))),
        (STRUCT(CAST('metrics' AS STRING), DATETIME('2022-12-12 11:46:00')))
      ]
    )
  )
