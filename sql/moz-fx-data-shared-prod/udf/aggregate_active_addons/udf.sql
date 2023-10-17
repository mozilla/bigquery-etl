/*

This function selects most frequently occuring value for each addon_id, using
the latest value in the input among ties. The type for active_addons is
ARRAY<STRUCT<addon_id STRING, ...>>, i.e. the output of
`SELECT ARRAY_CONCAT_AGG(active_addons) FROM telemetry.main_summary_v4`, and
is left unspecified to allow changes to the fields of the STRUCT.

*/
CREATE OR REPLACE FUNCTION udf.aggregate_active_addons(active_addons ANY TYPE) AS (
  ARRAY(
    SELECT
      mozfun.json.mode_last(ARRAY_AGG(element))
    FROM
      UNNEST(active_addons) AS element
    GROUP BY
      element.addon_id
  )
);

-- Tests
SELECT
  mozfun.assert.array_equals(
    [STRUCT('d' AS addon_id), STRUCT('b' AS addon_id), STRUCT('a' AS addon_id)],
    udf.aggregate_active_addons(
      [
        STRUCT('d' AS addon_id),
        STRUCT('b' AS addon_id),
        STRUCT('a' AS addon_id),
        STRUCT('b' AS addon_id)
      ]
    )
  )
