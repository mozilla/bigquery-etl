/*

Add fields from additional_attributes to active_addons in main_v4.

Returns an array instead of a "map" for backwards compatibility.

The fields from additional_attributes due to union types: integer or boolean
for foreignInstall and userDisabled; string or number for version.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c00df191501b39d2c4e2ece3fe703a0ef3/src/main/scala/com/mozilla/telemetry/views/MainSummaryView.scala#L422-L449

*/

CREATE TEMP FUNCTION udf_js_main_summary_active_addons(
  active_addons ARRAY<STRUCT<
    key STRING,
    value STRUCT<
      app_disabled BOOL,
      blocklisted BOOL,
      description STRING,
      has_binary_components BOOL,
      install_day INT64,
      is_system BOOL,
      name STRING,
      scope INT64,
      signed_state INT64,
      type STRING,
      update_day INT64,
      is_web_extension BOOL,
      multiprocess_compatible BOOL
    >
  >>,
  active_addons_json STRING
)
RETURNS ARRAY<STRUCT<
  addon_id STRING,
  blocklisted BOOL,
  name STRING,
  user_disabled BOOL,
  app_disabled BOOL,
  version STRING,
  scope INT64,
  type STRING,
  foreign_install BOOL,
  has_binary_components BOOL,
  install_day INT64,
  update_day INT64,
  signed_state INT64,
  is_system BOOL,
  is_web_extension BOOL,
  multiprocess_compatible BOOL
>>
LANGUAGE js AS """
const additional_properties = JSON.parse(active_addons_json) || {};
const result = [];
(active_addons || []).forEach((item) => {
  const addon_json = additional_properties[item.key] || {};
  const value = item.value || {};
  result.push({
    addon_id: item.key,
    blocklisted: value.blocklisted,
    name: value.name,
    user_disabled: addon_json.userDisabled,
    app_disabled: value.app_disabled,
    version: addon_json.version,
    scope: value.scope,
    type: value.type,
    foreign_install: addon_json.foreignInstall,
    has_binary_components: value.has_binary_components,
    install_day: value.install_day,
    update_day: value.update_day,
    signed_state: value.signed_state,
    is_system: value.is_system,
    is_web_extension: value.is_web_extension,
    multiprocess_compatible: value.multiprocess_compatible,
  });
});
return result;
""";
-- Tests
WITH result AS (
  SELECT AS VALUE
    udf_js_main_summary_active_addons(
      [
        (
          'addon_id',
          (TRUE, TRUE, '', TRUE, 2, TRUE, 'name', 1, 4, 'type', 3, TRUE, TRUE)
        )
      ],
      '{"addon_id":{"version":"version","userDisabled":true,"foreignInstall":true}}'
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
