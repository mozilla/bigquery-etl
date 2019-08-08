/*

This function selects most frequently occuring value for each addon_id, using
the latest value in the input among ties. The type for nested_active_addons is
ARRAY<STRUCT<list ARRAY<STRUCT<element STRUCT<addon_id STRING, ...>>>>>, i.e.
the output of `SELECT ARRAY_AGG(active_addons) FROM main_summary_v4`, and
is left unspecified to allow changes to the element STRUCT.

The type for active_addons is the result of ARRAY<ANY TYPE> becoming
nested as STRUCT<list ARRAY<STRUCT<element ANY TYPE>>> when parquet is loaded
into BigQuery, such as with main_summary_v4.

*/

CREATE TEMP FUNCTION
  udf_aggregate_active_addons(active_addons ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        STRUCT(udf_json_mode_last(ARRAY_AGG(element)) AS element)
      FROM
        UNNEST(active_addons)
      GROUP BY
        element.addon_id) AS list));
