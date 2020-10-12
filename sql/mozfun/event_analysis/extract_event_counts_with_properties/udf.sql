CREATE OR REPLACE FUNCTION event_analysis.extract_event_counts_with_properties(events STRING)
RETURNS ARRAY<
  STRUCT<event_index STRING, property_index INT64, property_value_index STRING, count INT64>
> AS (
  ARRAY(
    SELECT AS STRUCT
      SUBSTR(event_with_props, -1, 1) AS event_index,
      IF(COALESCE(off, 0) = 0, NULL, off) AS property_index,
      IF(COALESCE(off, 0) = 0, NULL, property) AS property_value_index,
      COUNT(*) AS count
    FROM
      UNNEST(SPLIT(events, ',')) AS event_with_props
    LEFT JOIN
      UNNEST(SPLIT(REVERSE(event_with_props), '')) AS property
      WITH OFFSET off
    WHERE
      LENGTH(event_with_props) > 0
    GROUP BY
      event_index,
      property_index,
      property_value_index
    ORDER BY
      event_index ASC,
      property_index ASC,
      property_value_index ASC
  )
);

-- Tests
WITH results AS (
  SELECT
    event_analysis.extract_event_counts_with_properties('a,a,b,') AS multi,
    event_analysis.extract_event_counts_with_properties('b,b,b,') AS single,
    event_analysis.extract_event_counts_with_properties('aa,ba,ca,')
  AS
    with_event_props,
    event_analysis.extract_event_counts_with_properties('aaaa,aab,')
  AS
    with_many_props,
)
SELECT
  assert.struct_equals(
    STRUCT(
      'a' AS event_index,
      NULL AS property_index,
      CAST(NULL AS STRING) AS property_value_index,
      2 AS count
    ),
    multi[OFFSET(0)]
  ),
  assert.struct_equals(
    STRUCT(
      'b' AS event_index,
      NULL AS property_index,
      CAST(NULL AS STRING) AS property_value_index,
      1 AS count
    ),
    multi[OFFSET(1)]
  ),
  assert.struct_equals(
    STRUCT(
      'b' AS event_index,
      NULL AS property_index,
      CAST(NULL AS STRING) AS property_value_index,
      3 AS count
    ),
    single[OFFSET(0)]
  ),
  assert.equals(4, ARRAY_LENGTH(with_event_props)),
  assert.struct_equals(
    STRUCT(
      'a' AS event_index,
      NULL AS property_index,
      CAST(NULL AS STRING) AS property_value_index,
      3 AS count
    ),
    with_event_props[OFFSET(0)]
  ),
  assert.struct_equals(
    STRUCT('a' AS event_index, 1 AS property_index, 'a' AS property_value_index, 1 AS count),
    with_event_props[OFFSET(1)]
  ),
  assert.struct_equals(
    STRUCT('a' AS event_index, 1 AS property_index, 'b' AS property_value_index, 1 AS count),
    with_event_props[OFFSET(2)]
  ),
  assert.struct_equals(
    STRUCT('a' AS event_index, 1 AS property_index, 'c' AS property_value_index, 1 AS count),
    with_event_props[OFFSET(3)]
  ),
  assert.equals(7, ARRAY_LENGTH(with_many_props)),
  assert.struct_equals(
    STRUCT(
      'a' AS event_index,
      NULL AS property_index,
      CAST(NULL AS STRING) AS property_value_index,
      1 AS count
    ),
    with_many_props[OFFSET(0)]
  ),
  assert.struct_equals(
    STRUCT('a' AS event_index, 1 AS property_index, 'a' AS property_value_index, 1 AS count),
    with_many_props[OFFSET(1)]
  ),
  assert.struct_equals(
    STRUCT('a' AS event_index, 2 AS property_index, 'a' AS property_value_index, 1 AS count),
    with_many_props[OFFSET(2)]
  ),
  assert.struct_equals(
    STRUCT('a' AS event_index, 3 AS property_index, 'a' AS property_value_index, 1 AS count),
    with_many_props[OFFSET(3)]
  ),
  assert.struct_equals(
    STRUCT(
      'b' AS event_index,
      NULL AS property_index,
      CAST(NULL AS STRING) AS property_value_index,
      1 AS count
    ),
    with_many_props[OFFSET(4)]
  ),
  assert.struct_equals(
    STRUCT('b' AS event_index, 1 AS property_index, 'a' AS property_value_index, 1 AS count),
    with_many_props[OFFSET(5)]
  ),
  assert.struct_equals(
    STRUCT('b' AS event_index, 2 AS property_index, 'a' AS property_value_index, 1 AS count),
    with_many_props[OFFSET(6)]
  ),
FROM
  results
