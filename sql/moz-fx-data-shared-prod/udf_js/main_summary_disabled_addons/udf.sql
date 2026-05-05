/*

Report the ids of the addons which are in the addonDetails but not in the
activeAddons.  They are the disabled addons (possibly because they are legacy).
We need this as addonDetails may contain both disabled and active addons.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c00df191501b39d2c4e2ece3fe703a0ef3/src/main/scala/com/mozilla/telemetry/views/MainSummaryView.scala#L451-L464

*/
CREATE OR REPLACE FUNCTION udf_js.main_summary_disabled_addons(
  active_addon_ids ARRAY<STRING>,
  addon_details_json STRING
)
RETURNS ARRAY<STRING> DETERMINISTIC
LANGUAGE js
AS
  """
try {
  const addonDetails = Object.keys(JSON.parse(addon_details_json) || {});
  return addonDetails.filter(k => !(active_addon_ids || []).includes(k));
} catch(err) {
  return null;
}
""";

-- Tests
SELECT
  mozfun.assert.array_equals(
    udf_js.main_summary_disabled_addons(["foo", "baz", "buz"], '{"foo":{}, "buz":{}, "blee":{}}'),
    ["blee"]
  ),
  mozfun.assert.array_equals(udf_js.main_summary_disabled_addons(NULL, '{"foo":{}}'), ["foo"])
