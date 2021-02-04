/*
Merge an array of histograms into a single histogram.
- The histogram values will be summed per-bucket
- The count will be summed
- Other fields will take the mode_last
*/
CREATE OR REPLACE FUNCTION hist.merge(histogram_list ANY TYPE) AS (
  STRUCT(
    stats.mode_last(ARRAY(SELECT bucket_count FROM UNNEST(histogram_list))) AS bucket_count,
    (SELECT SUM(`sum`) FROM UNNEST(histogram_list)) AS `sum`,
    stats.mode_last(ARRAY(SELECT histogram_type FROM UNNEST(histogram_list))) AS histogram_type,
    [
      stats.mode_last(ARRAY(SELECT `range`[SAFE_OFFSET(0)] FROM UNNEST(histogram_list))),
      stats.mode_last(ARRAY(SELECT `range`[SAFE_OFFSET(1)] FROM UNNEST(histogram_list)))
    ] AS `range`,
    ARRAY(
      SELECT AS STRUCT
        key,
        SUM(value) AS value
      FROM
        UNNEST(histogram_list) AS histogram,
        UNNEST(VALUES)
      GROUP BY
        key
    ) AS values
  )
);

-- Test
WITH histograms AS (
  SELECT
    STRUCT(
      5 AS bucket_count,
      20 AS `sum`,
      1 AS histogram_type,
      [0, 100] AS `range`,
      [STRUCT(0 AS key, 0 AS value), STRUCT(20 AS key, 1 AS value)] AS values
    ) AS h,
  UNION ALL
  SELECT
    STRUCT(
      5 AS bucket_count,
      40 AS `sum`,
      1 AS histogram_type,
      [0, 100] AS `range`,
      [STRUCT(0 AS key, 0 AS value), STRUCT(40 AS key, 1 AS value)] AS values
    )
),
merged AS (
  SELECT
    hist.merge(ARRAY_AGG(h)) AS h
  FROM
    histograms
)
SELECT
  assert.histogram_equals(
    STRUCT(
      5 AS bucket_count,
      60 AS `sum`,
      1 AS histogram_type,
      [0, 100] AS `range`,
      [STRUCT(0 AS key, 0 AS value), STRUCT(20, 1), STRUCT(40, 1)] AS values
    ),
    h
  )
FROM
  merged;
