/*
Return the most frequent STRUCT from an array, breaking ties by latest occurrence
(i.e., mode_last_retain_nulls over whole structs). Use to keep related fields aggregated together.
See also: `map.mode_last`, which determines each value using `stats.mode_last`.
*/
CREATE OR REPLACE FUNCTION map.mode_last_struct_retain_nulls(entries ANY TYPE) AS (
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
  assert.struct_equals(
    STRUCT(
      'Berlin' AS city,
      'BE' AS subdivision1,
      CAST(NULL AS STRING) AS subdivision2,
      'DE' AS country
    ),
    map.mode_last_struct_retain_nulls(
      [
        STRUCT(
          'Berlin' AS city,
          'BE' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT(
          'Munich' AS city,
          'BY' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT(
          'Berlin' AS city,
          'BE' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        )
      ]
    )
  ),
  -- 2) Tie -> latest wins (Berlin x2, Munich x2, last element is Munich)
  assert.struct_equals(
    STRUCT(
      'Munich' AS city,
      'BY' AS subdivision1,
      CAST(NULL AS STRING) AS subdivision2,
      'DE' AS country
    ),
    map.mode_last_struct_retain_nulls(
      [
        STRUCT(
          'Berlin' AS city,
          'BE' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT(
          'Munich' AS city,
          'BY' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT(
          'Berlin' AS city,
          'BE' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT(
          'Munich' AS city,
          'BY' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        )  -- latest among the tied
      ]
    )
  ),
  -- 3) FULL-struct equality: different subdivision2 means different value
  assert.struct_equals(
    STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'A' AS subdivision2, 'DE' AS country),
    map.mode_last_struct_retain_nulls(
      [
        STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'A' AS subdivision2, 'DE' AS country),
        STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'B' AS subdivision2, 'DE' AS country),
        STRUCT('Berlin' AS city, 'BE' AS subdivision1, 'A' AS subdivision2, 'DE' AS country)
      ]
    )
  ),
  -- 4) Single element returns itself
  assert.struct_equals(
    STRUCT(
      'Cologne' AS city,
      'NW' AS subdivision1,
      CAST(NULL AS STRING) AS subdivision2,
      'DE' AS country
    ),
    map.mode_last_struct_retain_nulls(
      [
        STRUCT(
          'Cologne' AS city,
          'NW' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        )
      ]
    )
  ),
-- 5) Tie between NULL struct and a non-NULL struct; latest wins -> expect Berlin
  assert.struct_equals(
    STRUCT(
      'Berlin' AS city,
      'BE' AS subdivision1,
      CAST(NULL AS STRING) AS subdivision2,
      'DE' AS country
    ),
    map.mode_last_struct_retain_nulls(
      [
        STRUCT(
          CAST(NULL AS STRING) AS city,
          CAST(NULL AS STRING) AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          CAST(NULL AS STRING) AS country
        ),
        STRUCT(
          'Berlin' AS city,
          'BE' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT(
          CAST(NULL AS STRING) AS city,
          CAST(NULL AS STRING) AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          CAST(NULL AS STRING) AS country
        ),
        STRUCT(
          'Berlin' AS city,
          'BE' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        )   -- latest among the tied
      ]
    )
  ),
-- 6) NULL struct occurs most frequently -> expect NULL
  assert.struct_equals(
    STRUCT(
      CAST(NULL AS STRING) AS city,
      CAST(NULL AS STRING) AS subdivision1,
      CAST(NULL AS STRING) AS subdivision2,
      CAST(NULL AS STRING) AS country
    ),
    map.mode_last_struct_retain_nulls(
      [
        STRUCT(
          CAST(NULL AS STRING) AS city,
          CAST(NULL AS STRING) AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          CAST(NULL AS STRING) AS country
        ),
        STRUCT(
          'Berlin' AS city,
          'BE' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT(
          CAST(NULL AS STRING) AS city,
          CAST(NULL AS STRING) AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          CAST(NULL AS STRING) AS country
        )
      ]
    )
  ),
-- 7) City is NULL but other fields present; that exact struct is most frequent -> expect that struct (with city = NULL)
  assert.struct_equals(
    STRUCT(
      CAST(NULL AS STRING) AS city,
      'BY' AS subdivision1,
      CAST(NULL AS STRING) AS subdivision2,
      'DE' AS country
    ),
    map.mode_last_struct_retain_nulls(
      [
        STRUCT(
          CAST(NULL AS STRING) AS city,
          'BY' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        ),
        STRUCT('Berlin' AS city, 'BE' AS subdivision1, NULL AS subdivision2, 'DE' AS country),
        STRUCT(
          CAST(NULL AS STRING) AS city,
          'BY' AS subdivision1,
          CAST(NULL AS STRING) AS subdivision2,
          'DE' AS country
        )
      ]
    )
  );
