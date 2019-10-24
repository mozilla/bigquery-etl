/*

Add fields from additional_attributes to active_addons in main_v4.

Returns an array instead of a "map" for backwards compatibility.

*/
CREATE TEMP FUNCTION udf_main_summary_active_addons(active_addons ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key AS addon_id,
      value.blocklisted,
      value.name,
      value.user_disabled > 0 AS user_disabled,
      value.app_disabled,
      value.version,
      value.scope,
      value.type,
      value.foreign_install > 0 AS foreign_install,
      value.has_binary_components,
      value.install_day,
      value.update_day,
      value.signed_state,
      value.is_system,
      value.is_web_extension,
      value.multiprocess_compatible
    FROM
      UNNEST(active_addons)
  )
);
-- Tests
WITH result AS (
  SELECT AS VALUE
    udf_main_summary_active_addons(
      [
        STRUCT(
          'addon_id' AS key,
          STRUCT(
            TRUE AS app_disabled,
            TRUE AS blocklisted,
            '' AS description,
            TRUE AS has_binary_components,
            2 AS install_day,
            TRUE AS is_system,
            'name' AS name,
            1 AS scope,
            4 AS signed_state,
            'type' AS type,
            3 AS update_day,
            TRUE AS is_web_extension,
            TRUE AS multiprocess_compatible,
            1 AS user_disabled,
            "version" AS version,
            1 AS foreign_install
          ) AS value
        )
      ]
    )
)
SELECT
  assert_equals(1, ARRAY_LENGTH(result)),
  assert_equals(
    ('addon_id', TRUE, 'name', TRUE, TRUE, 'version', 1, 'type', TRUE, TRUE, 2, 3, 4, TRUE, TRUE, TRUE),
    result[OFFSET(0)]
  )
FROM
  result
