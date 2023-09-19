CREATE OR REPLACE FUNCTION norm.get_earliest_value(
  value_set ARRAY<STRUCT<value STRING, value_source STRING, value_date DATETIME>>
)
RETURNS STRUCT<earliest_value STRING, earliest_value_source STRING, earliest_date DATETIME> AS (
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
      ARRAY_AGG(STRUCT(value, value_source, value_date) ORDER BY value_date ASC)[SAFE_OFFSET(0)]
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
      'XYZ' AS earliest_value,
      'metrics_ping' AS earliest_value_source,
      DATETIME('2021-12-12T11:45:00') AS earliest_date
    ),
    norm.get_earliest_value(
      [
        (STRUCT(CAST(NULL AS STRING), CAST(NULL AS STRING), DATETIME('2021-11-01 12:00:00'))),
        (
          STRUCT(
            CAST('MYN' AS STRING),
            CAST('first_session_ping' AS STRING),
            DATETIME('2022-01-01 11:55:00')
          )
        ),
        (
          STRUCT(
            CAST('XYZ' AS STRING),
            CAST('metrics_ping' AS STRING),
            DATETIME('2021-12-12 11:45:00')
          )
        )
      ]
    )
  ),
  assert.equals(
    STRUCT(
      'keep' AS earliest_value,
      'main_ping' AS earliest_value_source,
      DATETIME('2019-01-01T12:00:00') AS earliest_date
    ),
    norm.get_earliest_value(
      [
        (STRUCT(CAST('keep' AS STRING), 'main_ping', DATETIME('2019-01-01 12:00:00'))),
        (STRUCT(CAST('remove' AS STRING), CAST(NULL AS STRING), CAST(NULL AS DATETIME))),
        (STRUCT(CAST('rewrite' AS STRING), 'shutdown_ping', DATETIME('2021-12-12 12:00:00')))
      ]
    )
  ),
  assert.equals(
    STRUCT(
      CAST('2022-12-12' AS STRING) AS earliest_value,
      'first_session' AS earliest_value_source,
      DATETIME('2022-12-12T11:45:00') AS earliest_date
    ),
    norm.get_earliest_value(
      [
        (
          STRUCT(
            CAST('2022-12-12' AS STRING),
            CAST(NULL AS STRING),
            DATETIME('2022-12-12 12:00:00')
          )
        ),
        (
          STRUCT(
            CAST('2022-12-12' AS STRING),
            CAST('first_session' AS STRING),
            DATETIME('2022-12-12 11:45:00')
          )
        ),
        (
          STRUCT(
            CAST('2022-12-12' AS STRING),
            CAST('metrics' AS STRING),
            DATETIME('2022-12-12 11:46:00')
          )
        )
      ]
    )
  ),
  assert.equals(
    STRUCT(
      CAST('2022-12-13' AS STRING) AS earliest_value,
      'first_session' AS earliest_value_source,
      DATETIME('2022-12-13T00:00:00') AS earliest_date
    ),
    norm.get_earliest_value(
      [
        (STRUCT(CAST('2022-12-14' AS STRING), CAST(NULL AS STRING), DATETIME('2022-12-14'))),
        (
          STRUCT(
            CAST('2022-12-13' AS STRING),
            CAST('first_session' AS STRING),
            DATETIME('2022-12-13')
          )
        ),
        (STRUCT(CAST('2022-12-13' AS STRING), CAST('abc' AS STRING), DATETIME('2022-12-13')))
      ]
    )
  ),
  assert.null(
    norm.get_earliest_value(
      [
        (STRUCT(CAST('ABC' AS STRING), CAST(NULL AS STRING), CAST(NULL AS DATETIME))),
        (STRUCT(CAST('DEF' AS STRING), CAST(NULL AS STRING), CAST(NULL AS DATETIME))),
        (STRUCT(CAST('GHI' AS STRING), CAST(NULL AS STRING), CAST(NULL AS DATETIME)))
      ]
    )
  )
