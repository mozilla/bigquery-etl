/*
Return the most frequent STRUCT from an array, breaking ties by latest occurrence
(i.e., mode_last over whole structs). Use to keep related fields aggregated together.
See also: `map.mode_last`, which determines each value using `stats.mode_last`.
*/
CREATE OR REPLACE FUNCTION map.mode_last_struct(entries ANY TYPE) AS (
  (
    SELECT AS STRUCT
      s.*
    FROM
      (
        SELECT
          s,
          COUNT(*) AS freq,
          MAX(pos) AS last_pos
        FROM
          UNNEST(entries) AS s
          WITH OFFSET pos
        GROUP BY
          s
      )
    ORDER BY
      freq DESC,
      last_pos DESC
    LIMIT
      1
  )
);

-- Tests
SELECT
  -- 1) Most frequent wins (Berlin appears twice)
  assert.equals(
    TO_JSON_STRING(
      STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country)
    ),
    TO_JSON_STRING(
      map.mode_last_struct(
        [
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          STRUCT('Munich' AS city, 'BY' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country)
        ]
      )
    )
  ),
  -- 2) Tie -> latest wins (Berlin x2, Munich x2, last element is Munich)
  assert.equals(
    TO_JSON_STRING(
      STRUCT(
        'Munich' AS city,
        'BY' AS subdivision1,
        CAST(NULL AS STRING) AS subdivision2,
        'DE' AS country
      )
    ),
    TO_JSON_STRING(
      map.mode_last_struct(
        [
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          STRUCT('Munich' AS city, 'BY' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          STRUCT(
            'Munich' AS city,
            'BY' AS subdivision1,
            NULL AS subdivision2,
            'DE' AS country
          )  -- latest among the tied
        ]
      )
    )
  ),
  -- 3) FULL-struct equality: different subdivision2 means different value
  assert.equals(
    TO_JSON_STRING(
      STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'A' AS subdivision2, 'DE' AS country)
    ),
    TO_JSON_STRING(
      map.mode_last_struct(
        [
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'A' AS subdivision2, 'DE' AS country),
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'B' AS subdivision2, 'DE' AS country),
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'A' AS subdivision2, 'DE' AS country)
        ]
      )
    )
  ),
  -- 4) Single element returns itself
  assert.equals(
    TO_JSON_STRING(
      STRUCT(
        'Cologne' AS city,
        'NW' AS subdivision1,
        CAST(NULL AS STRING) AS subdivision2,
        'DE' AS country
      )
    ),
    TO_JSON_STRING(
      map.mode_last_struct(
        [STRUCT('Cologne' AS city, 'NW' AS subdivision1, NULL AS subdivision2, 'DE' AS country)]
      )
    )
  ),
-- 5) Tie between NULL struct and a non-NULL struct; latest wins -> expect Berlin
  assert.equals(
    TO_JSON_STRING(
      STRUCT(
        'Berlin' AS city,
        'BE' AS subdivision1,
        CAST(NULL AS STRING) AS subdivision2,
        'DE' AS country
      )
    ),
    TO_JSON_STRING(
      map.mode_last_struct(
        [
          CAST(
            NULL
            AS
              STRUCT<city STRING, subdivision1 STRING, subdivision2 STRING, country STRING>
          ),
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          CAST(
            NULL
            AS
              STRUCT<city STRING, subdivision1 STRING, subdivision2 STRING, country STRING>
          ),
          STRUCT(
            'Berlin' AS city,
            'BE' AS subdivision1,
            NULL AS subdivision2,
            'DE' AS country
          )   -- latest among the tied
        ]
      )
    )
  ),
-- 6) NULL struct occurs most frequently -> expect NULL
  assert.equals(
    TO_JSON_STRING(CAST(NULL AS STRING)),
    TO_JSON_STRING(
      map.mode_last_struct(
        [
          CAST(
            NULL
            AS
              STRUCT<city STRING, subdivision1 STRING, subdivision2 STRING, country STRING>
          ),
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          CAST(
            NULL
            AS
              STRUCT<city STRING, subdivision1 STRING, subdivision2 STRING, country STRING>
          )
        ]
      )
    )
  ),
-- 7) City is NULL but other fields present; that exact struct is most frequent -> expect that struct (with city = NULL)
  assert.equals(
    TO_JSON_STRING(
      STRUCT(
        CAST(NULL AS STRING) AS city,
        'BY' AS subdivision1,
        CAST(NULL AS STRING) AS subdivision2,
        'DE' AS country
      )
    ),
    TO_JSON_STRING(
      map.mode_last_struct(
        [
          STRUCT(NULL AS city, 'BY' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
          STRUCT(NULL AS city, 'BY' AS subdivision1, NULL AS subdivision2, 'DE' AS country)
        ]
      )
    )
  );
