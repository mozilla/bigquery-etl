CREATE OR REPLACE FUNCTION udf.search_counts_map_sum(
  entries ARRAY<STRUCT<engine STRING, source STRING, count INT64>>
) AS (
  ARRAY(
    SELECT AS STRUCT
      engine,
      source,
      SUM(count) AS count
    FROM
      UNNEST(entries)
    GROUP BY
      engine,
      source
  )
);

-- Tests
SELECT
  mozfun.assert.array_equals(
    ARRAY<STRUCT<engine STRING, source STRING, count INT64>>[],
    udf.search_counts_map_sum(ARRAY<STRUCT<engine STRING, source STRING, count INT64>>[])
  ),
  mozfun.assert.array_equals(
    ARRAY<STRUCT<engine STRING, source STRING, count INT64>>[
      ('engine1', 'urlbar', 3),
      ('engine1', 'contextmenu', 7),
      ('engine2', 'urlbar', 5)
    ],
    udf.search_counts_map_sum(
      ARRAY<STRUCT<engine STRING, source STRING, count INT64>>[
        ('engine1', 'urlbar', 1),
        ('engine1', 'contextmenu', 3),
        ('engine1', 'contextmenu', 4),
        ('engine1', 'urlbar', 2),
        ('engine2', 'urlbar', 5)
      ]
    )
  ),
