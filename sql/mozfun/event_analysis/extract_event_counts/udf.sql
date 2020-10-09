CREATE OR REPLACE FUNCTION event_analysis.extract_event_counts(events STRING)
RETURNS ARRAY<STRUCT<index STRING, count INT64>> AS (
  ARRAY(
    SELECT AS STRUCT
      index,
      COUNT(*) AS count
    FROM
      UNNEST(regexp_extract_all(events, r'(.),')) AS index
    GROUP BY
      index
    ORDER BY
      index ASC
  )
);

-- Tests
WITH results AS (
  SELECT
    event_analysis.extract_event_counts('a,a,b,') AS multi,
    event_analysis.extract_event_counts('b,b,b,') AS single,
    event_analysis.extract_event_counts('aa,ab,ac,')
  AS
    with_event_props
)
SELECT
  assert.equals('a', multi[OFFSET(0)].index),
  assert.equals(2, multi[OFFSET(0)].count),
  assert.equals('b', multi[OFFSET(1)].index),
  assert.equals(1, multi[OFFSET(1)].count),
  assert.equals('b', single[OFFSET(0)].index),
  assert.equals(3, single[OFFSET(0)].count),
  assert.equals('a', with_event_props[OFFSET(0)].index),
  assert.equals(1, with_event_props[OFFSET(0)].count),
  assert.equals('b', with_event_props[OFFSET(1)].index),
  assert.equals(1, with_event_props[OFFSET(1)].count),
  assert.equals('c', with_event_props[OFFSET(2)].index),
  assert.equals(1, with_event_props[OFFSET(2)].count),
FROM
  results
