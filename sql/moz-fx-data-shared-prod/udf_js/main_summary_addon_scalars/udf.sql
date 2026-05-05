/*

Parse scalars from payload.processes.dynamic into map columns for each value type.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c00df191501b39d2c4e2ece3fe703a0ef3/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L385-L399

*/
CREATE OR REPLACE FUNCTION udf_js.main_summary_addon_scalars(
  dynamic_scalars_json STRING,
  dynamic_keyed_scalars_json STRING
)
RETURNS STRUCT<
  keyed_boolean_addon_scalars ARRAY<
    STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value BOOL>>>
  >,
  keyed_uint_addon_scalars ARRAY<STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value INT64>>>>,
  string_addon_scalars ARRAY<STRUCT<key STRING, value STRING>>,
  keyed_string_addon_scalars ARRAY<
    STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value STRING>>>
  >,
  uint_addon_scalars ARRAY<STRUCT<key STRING, value INT64>>,
  boolean_addon_scalars ARRAY<STRUCT<key STRING, value BOOL>>
> DETERMINISTIC
LANGUAGE js
AS
  """
try {
  const dynamicScalars = JSON.parse(dynamic_scalars_json) || {};
  const dynamicKeyedScalars = JSON.parse(dynamic_keyed_scalars_json) || {};
  const result = {
    keyed_boolean_addon_scalars: [],
    keyed_uint_addon_scalars: [],
    string_addon_scalars: [],
    keyed_string_addon_scalars: [],
    uint_addon_scalars: [],
    boolean_addon_scalars: [],
  };
  const typeMap = {
    number: 'uint',
    boolean: 'boolean',
    string: 'string',
  };
  Object.entries(dynamicKeyedScalars).forEach(([k, v]) => {
    const type = typeMap[typeof Object.values(v)[0]];
    const column = `keyed_${type}_addon_scalars`;
    result[column].push({
      key: k,
      value: Object.entries(v).map(([key, value]) => ({ key, value }))
    });
  });
  Object.entries(dynamicScalars).forEach(([k, v]) => {
    const type = typeMap[typeof v];
    result[`${type}_addon_scalars`].push({key: k, value: v});
  });
  return result;
} catch(err) {
  return null;
}
""";

-- Tests
-- format:off
SELECT
  mozfun.assert.equals(2, ARRAY_LENGTH(keyed_boolean_addon_scalars)),
  mozfun.assert.equals("foo1", keyed_boolean_addon_scalars[OFFSET(0)].key),
  mozfun.assert.array_equals([STRUCT("a" as key, true as value), STRUCT("b" as key, false as value)], keyed_boolean_addon_scalars[OFFSET(0)].value),
  mozfun.assert.equals("foo7", keyed_boolean_addon_scalars[OFFSET(1)].key),
  mozfun.assert.array_equals([STRUCT("c", false), STRUCT("d", true)], keyed_boolean_addon_scalars[OFFSET(1)].value),
  mozfun.assert.equals(1, ARRAY_LENGTH(keyed_uint_addon_scalars)),
  mozfun.assert.equals("foo2", keyed_uint_addon_scalars[OFFSET(0)].key),
  mozfun.assert.array_equals([STRUCT("a", 17), STRUCT("b", 42)], keyed_uint_addon_scalars[OFFSET(0)].value),
  mozfun.assert.equals(1, ARRAY_LENGTH(string_addon_scalars)),
  mozfun.assert.equals("foo3", string_addon_scalars[OFFSET(0)].key),
  mozfun.assert.equals("blee", string_addon_scalars[OFFSET(0)].value),
  mozfun.assert.equals(1, ARRAY_LENGTH(keyed_string_addon_scalars)),
  mozfun.assert.equals("foo4", keyed_string_addon_scalars[OFFSET(0)].key),
  mozfun.assert.array_equals([STRUCT("a", "yes"), STRUCT("b", "no")], keyed_string_addon_scalars[OFFSET(0)].value),
  mozfun.assert.equals(1, ARRAY_LENGTH(uint_addon_scalars)),
  mozfun.assert.equals("foo5", uint_addon_scalars[OFFSET(0)].key),
  mozfun.assert.equals(17, uint_addon_scalars[OFFSET(0)].value),
  mozfun.assert.equals(1, ARRAY_LENGTH(boolean_addon_scalars)),
  mozfun.assert.equals("foo6", boolean_addon_scalars[OFFSET(0)].key),
  mozfun.assert.equals(false, boolean_addon_scalars[OFFSET(0)].value)
FROM (
  SELECT
    udf_js.main_summary_addon_scalars(
      '{"foo3": "blee", "foo5": 17, "foo6": false}',
      '{"foo1": {"a": true, "b": false}, "foo2": {"a": 17, "b": 42}, "foo4": {"a": "yes", "b": "no"}, "foo7": {"c": false, "d": true}}'
    ).*
)
-- format:on
