/*

Sum up the monthy search count arrays by normalized engine

 */
CREATE OR REPLACE FUNCTION udf.normalize_monthly_searches(
  engine_searches ARRAY<
    STRUCT<
      key STRING,
      value STRUCT<
        total_searches ARRAY<INT64>,
        tagged_searches ARRAY<INT64>,
        search_with_ads ARRAY<INT64>,
        ad_click ARRAY<INT64>
      >
    >
  >
) AS (
  ARRAY(
    SELECT AS STRUCT
      udf.normalize_search_engine(engine.key) AS key,
      udf.add_searches_by_index(
        ARRAY_AGG(
          STRUCT(
            total_searches,
            engine.value.tagged_searches[OFFSET(index)],
            engine.value.search_with_ads[OFFSET(index)],
            engine.value.ad_click[OFFSET(index)],
            index
          )
        )
      ) AS value,
    FROM
      UNNEST(engine_searches) AS engine
    CROSS JOIN
      UNNEST(engine.value.total_searches) AS total_searches
      WITH OFFSET AS index
    GROUP BY
      key
    ORDER BY
      key
  )
);

-- Tests
WITH actual AS (
  SELECT
    udf.normalize_monthly_searches(
      ARRAY<
        STRUCT<
          key STRING,
          value STRUCT<
            total_searches ARRAY<INT64>,
            tagged_searches ARRAY<INT64>,
            search_with_ads ARRAY<INT64>,
            ad_clicks ARRAY<INT64>
          >
        >
      >[
        (
          CAST(NULL AS STRING),
          (
            GENERATE_ARRAY(11, 22),
            GENERATE_ARRAY(12, 23),
            GENERATE_ARRAY(13, 24),
            GENERATE_ARRAY(14, 25)
          )
        ),
        (
          CAST(NULL AS STRING),
          (
            GENERATE_ARRAY(13, 24),
            GENERATE_ARRAY(14, 25),
            GENERATE_ARRAY(15, 26),
            GENERATE_ARRAY(16, 27)
          )
        ),
        (
          'notarealengine',
          (
            GENERATE_ARRAY(1, 12),
            GENERATE_ARRAY(2, 13),
            GENERATE_ARRAY(3, 14),
            GENERATE_ARRAY(4, 15)
          )
        ),
        (
          'somefakeengine',
          (
            GENERATE_ARRAY(5, 16),
            GENERATE_ARRAY(6, 17),
            GENERATE_ARRAY(7, 18),
            GENERATE_ARRAY(8, 19)
          )
        )
      ]
    ) AS engine_searches
)
SELECT
  mozfun.assert.array_equals(GENERATE_ARRAY(24, 46, 2), searches.value.total_searches),
  mozfun.assert.array_equals(GENERATE_ARRAY(26, 48, 2), searches.value.tagged_searches),
  mozfun.assert.array_equals(GENERATE_ARRAY(28, 50, 2), searches.value.search_with_ads),
  mozfun.assert.array_equals(GENERATE_ARRAY(30, 52, 2), searches.value.ad_click),
FROM
  actual
CROSS JOIN
  UNNEST(engine_searches) searches
WHERE
  searches.key IS NULL
UNION ALL
SELECT
  mozfun.assert.array_equals(GENERATE_ARRAY(6, 28, 2), searches.value.total_searches),
  mozfun.assert.array_equals(GENERATE_ARRAY(8, 30, 2), searches.value.tagged_searches),
  mozfun.assert.array_equals(GENERATE_ARRAY(10, 32, 2), searches.value.search_with_ads),
  mozfun.assert.array_equals(GENERATE_ARRAY(12, 34, 2), searches.value.ad_click),
FROM
  actual
CROSS JOIN
  UNNEST(engine_searches) searches
WHERE
  searches.key = 'Other'
