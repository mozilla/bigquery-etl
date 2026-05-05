CREATE OR REPLACE FUNCTION udf.pack_event_properties(
  event_properties ANY TYPE,
  indices ANY TYPE
) AS (
  COALESCE(
    (
      SELECT
        STRING_AGG(
          COALESCE(
            mozfun.map.get_key(
              event_property.value,
              mozfun.map.get_key(event_properties, event_property.key)
            ),
            -- We use " to represent no associated for that event property
            '"'
          ),
          ''
          ORDER BY
            event_property.index DESC
        )
      FROM
        UNNEST(indices) AS event_property
    ),
    ''
  )
);

WITH examples AS (
  SELECT
    CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS null_property,
    CAST([] AS ARRAY<STRUCT<key STRING, value STRING>>) AS no_property,
    [STRUCT("property" AS key, "val1" AS value)] AS single_property,
    [
      STRUCT("property" AS key, "val0" AS value),
      STRUCT("property" AS key, "val1" AS value)
    ] AS duplicate_property,
    [STRUCT("second_property" AS key, "val" AS value)] AS secondary_property,
    [STRUCT("unknown_prop" AS key, "val" AS value)] AS unknown_property,
    [
      STRUCT(
        "property" AS key,
        [
          STRUCT("val0" AS key, "a" AS value, 1 AS index),
          STRUCT("val1" AS key, "b" AS value, 2 AS index)
        ] AS value,
        1 AS index
      ),
      STRUCT(
        "second_property" AS key,
        [STRUCT("val" AS key, "a" AS value, 1 AS index)] AS value,
        2 AS index
      )
    ] AS indices,
    CAST(
      []
      AS
        ARRAY<
          STRUCT<
            key STRING,
            index INT64,
            value ARRAY<STRUCT<key STRING, index INT64, value STRING>>
          >
        >
    ) AS empty_indices,
)
SELECT
  mozfun.assert.equals('""', udf.pack_event_properties(null_property, indices)),
  mozfun.assert.equals('""', udf.pack_event_properties(no_property, indices)),
  mozfun.assert.equals('"b', udf.pack_event_properties(single_property, indices)),
  mozfun.assert.equals('"a', udf.pack_event_properties(duplicate_property, indices)),
  mozfun.assert.equals('a"', udf.pack_event_properties(secondary_property, indices)),
  -- We specifically ignore unknown properties, this gives us flexibility during encoding
  mozfun.assert.equals('""', udf.pack_event_properties(unknown_property, indices)),
  mozfun.assert.equals('', udf.pack_event_properties(null_property, empty_indices)),
  mozfun.assert.equals('', udf.pack_event_properties(no_property, empty_indices)),
  mozfun.assert.equals('', udf.pack_event_properties(single_property, empty_indices)),
  mozfun.assert.equals('', udf.pack_event_properties(duplicate_property, empty_indices)),
  mozfun.assert.equals('', udf.pack_event_properties(secondary_property, empty_indices)),
  mozfun.assert.equals('', udf.pack_event_properties(unknown_property, empty_indices)),
FROM
  examples
