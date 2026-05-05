CREATE OR REPLACE FUNCTION udf.experiment_search_metric_to_array(
  metric ARRAY<STRUCT<key STRING, value INT64>>
) AS (
  JSON_EXTRACT_ARRAY(
    REGEXP_REPLACE(
      TO_JSON_STRING(REGEXP_EXTRACT_ALL(TO_JSON_STRING(metric), '"value":([^,}]+)')),
      r'"([^"]+)"',
      r'[\1, 0, 0]'
    )
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    udf.experiment_search_metric_to_array([STRUCT("engine1", 3)])[OFFSET(0)],
    "[3,0,0]"
  ),
  mozfun.assert.equals(
    udf.experiment_search_metric_to_array([STRUCT("engine1", 100), STRUCT("engine2", 4)])[
      OFFSET(0)
    ],
    "[100,0,0]"
  ),
  mozfun.assert.equals(
    udf.experiment_search_metric_to_array([STRUCT("engine1", 100), STRUCT("engine2", 4)])[
      OFFSET(1)
    ],
    "[4,0,0]"
  )
