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
    STRUCT('metrics', DATETIME('2021-12-12T12:00:00')),
    norm.get_earliest_value(
      [
        (STRUCT(CAST(NULL AS STRING), DATETIME('2022-01-01 11:00:00'))),
        (STRUCT('first_session', DATETIME('2022-01-01 12:00:00'))),
        (STRUCT('metrics', DATETIME('2021-12-12 12:00:00')))
      ]
    )
  ),
  assert.equals(
    STRUCT(NULL, NULL),
    norm.get_earliest_value(
      [
        (STRUCT(CAST(NULL AS STRING), DATETIME('2022-01-01 12:00:00'))),
        (STRUCT(CAST(NULL AS STRING), DATETIME('2022-01-01 12:00:00'))),
        (STRUCT(CAST(NULL AS STRING), DATETIME('2021-12-12 12:00:00')))
      ]
    )
  ),
  assert.equals(
    STRUCT('metrics', DATETIME('2021-12-12 12:00:00')),
    norm.get_earliest_value(
      [
        (STRUCT(CAST(NULL AS STRING), DATETIME('2020-01-01 12:00:00'))),
        (STRUCT('first_session', NULL)),
        (STRUCT('metrics', DATETIME('2021-12-12 12:00:00')))
      ]
    )
  ),
  assert.equals(
    STRUCT('existing', DATETIME('2019-01-01 12:00:00')),
    norm.get_earliest_value(
      [
        (STRUCT('existing', DATETIME('2019-01-01 12:00:00'))),
        (STRUCT('metrics', NULL)),
        (STRUCT('first_ping', DATETIME('2021-12-12 12:00:00')))
      ]
    )
  ),
  assert.equals(
    STRUCT('metrics', DATETIME('2021-12-12T12:00:00')),
    norm.get_earliest_value(
      [
        (STRUCT(CAST(NULL AS STRING), DATETIME('2022-01-01 12:00:00'))),
        (STRUCT('first_session', DATETIME('2023-01-01 12:00:00'))),
        (STRUCT('metrics', DATETIME('2021-12-12 12:00:00')))
      ]
    )
  )
