/*

Add fields from additional_attributes to active_addons in main_v4.

Return an array instead of a "map" for backwards compatibility.

The INT64 columns from BigQuery may be passed as strings, so parseInt before
returning them if they will be coerced to BOOL.

The fields from additional_attributes due to union types: integer or boolean
for foreignInstall and userDisabled; string or number for version.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c00df191501b39d2c4e2ece3fe703a0ef3/src/main/scala/com/mozilla/telemetry/views/MainSummaryView.scala#L422-L449

*/
CREATE OR REPLACE FUNCTION udf_js.main_summary_active_addons(
  active_addons ARRAY<
    STRUCT<
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
        multiprocess_compatible BOOL,
        foreign_install INT64,
        user_disabled INT64,
        version STRING
      >
    >
  >,
  active_addons_json STRING
)
RETURNS ARRAY<
  STRUCT<
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
  >
> DETERMINISTIC
LANGUAGE js
AS
  """
function ifnull(value1, value2) {
  // preserve falsey values and ignore missing values
  if (value1 !== null && value1 !== undefined) {
    return value1;
  }
  return value2;
}

function maybeParseInt(value) {
  // return null instead of NaN on failure
  result = parseInt(value);
  return isNaN(result) ? null : result;
}

try {
  const additional_properties = ifnull(JSON.parse(active_addons_json), {});
  const result = [];
  ifnull(active_addons, []).forEach((item) => {
    const addon_json = ifnull(additional_properties[item.key], {});
    const value = ifnull(item.value, {});
    result.push({
      addon_id: item.key,
      blocklisted: value.blocklisted,
      name: value.name,
      user_disabled: ifnull(maybeParseInt(value.user_disabled), addon_json.userDisabled),
      app_disabled: value.app_disabled,
      version: ifnull(value.version, addon_json.version),
      scope: value.scope,
      type: value.type,
      foreign_install: ifnull(maybeParseInt(value.foreign_install), addon_json.foreignInstall),
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
} catch(err) {
  return null;
}
""";

-- Tests
-- format:off
WITH result AS (
  SELECT AS VALUE
    ARRAY_CONCAT(
      udf_js.main_summary_active_addons(
        ARRAY<STRUCT<STRING,STRUCT<BOOL,BOOL,STRING,BOOL,INT64,BOOL,STRING,INT64,INT64,STRING,INT64,BOOL,BOOL,INT64,INT64,STRING>>>[
          -- truthy columns and additional_properties
          ('a', (TRUE, TRUE, 'description', TRUE, 2, TRUE, 'name', 1, 4, 'type', 3, TRUE, TRUE, NULL, NULL, NULL)),
          -- falsey columns and truthy additional_properties
          ('b', (FALSE, FALSE, '', FALSE, 0, FALSE, '', 0, 0, '', 0, FALSE, FALSE, NULL, NULL, NULL)),
          -- falsey columns and additional_properties
          ('c', (FALSE, FALSE, '', FALSE, 0, FALSE, '', 0, 0, '', 0, FALSE, FALSE, NULL, NULL, NULL)),
          -- truthy columns and falsey additional_properties
          ('d', (TRUE, TRUE, 'description', TRUE, 2, TRUE, 'name', 1, 4, 'type', 3, TRUE, TRUE, NULL, NULL, NULL)),
          -- truthy columns and missing additional_properties
          ('e', (TRUE, TRUE, 'description', TRUE, 2, TRUE, 'name', 1, 4, 'type', 3, TRUE, TRUE, 1, 1, "version")),
          -- falsey columns and missing additional_properties
          ('f', (FALSE, FALSE, '', FALSE, 0, FALSE, '', 0, 0, '', 0, FALSE, FALSE, 0, 0, "")),
          -- null columns and missing additional_properties
          ('g', (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)),
          -- null value and ignore additional_properties
          ('h', NULL),
          -- null value and truthy additional_properties
          ('i', NULL),
          -- null value and falsey additional_properties
          ('j', NULL)
          -- 'k' missing value and truthy additional_properties
        ],
        '''{
        "a":{"version":"version","userDisabled":true,"foreignInstall":true},
        "b":{"version":"version","userDisabled":1,"foreignInstall":1},
        "c":{"version":"","userDisabled":false,"foreignInstall":false},
        "d":{"version":"","userDisabled":0,"foreignInstall":0},
        "h":{"ignored":"ignored"},
        "i":{"version":"version","userDisabled":true,"foreignInstall":true},
        "j":{"version":"","userDisabled":false,"foreignInstall":false},
        "k":{"version":"version","userDisabled":true,"foreignInstall":true}
        }'''
      ),
      -- null additional properties
      udf_js.main_summary_active_addons(
        ARRAY<STRUCT<STRING,STRUCT<BOOL,BOOL,STRING,BOOL,INT64,BOOL,STRING,INT64,INT64,STRING,INT64,BOOL,BOOL,INT64,INT64,STRING>>>[
          -- truthy columns and null additional_properties
          ('l', (TRUE, TRUE, 'description', TRUE, 2, TRUE, 'name', 1, 4, 'type', 3, TRUE, TRUE, 1, 1, "version")),
          -- falsey columns and null additional_properties
          ('m', (FALSE, FALSE, '', FALSE, 0, FALSE, '', 0, 0, '', 0, FALSE, FALSE, 0, 0, "")),
          -- null columns and additional_properties
          ('n', NULL)
        ],
        NULL
      ),
      -- null addons and additional_properties
      udf_js.main_summary_active_addons(NULL, NULL),
      -- null addons and truthy additional_properties
      udf_js.main_summary_active_addons(NULL, '{"m":{"version":"version"}}')
    )
)
SELECT
  mozfun.assert.equals(13, ARRAY_LENGTH(result)),
  mozfun.assert.equals(('a', TRUE, 'name', TRUE, TRUE, 'version', 1, 'type', TRUE, TRUE, 2, 3, 4, TRUE, TRUE, TRUE), result[OFFSET(0)]),
  mozfun.assert.equals(('b', FALSE, '', TRUE, FALSE, 'version', 0, '', TRUE, FALSE, 0, 0, 0, FALSE, FALSE, FALSE), result[OFFSET(1)]),
  mozfun.assert.equals(('c', FALSE, '', FALSE, FALSE, '', 0, '', FALSE, FALSE, 0, 0, 0, FALSE, FALSE, FALSE), result[OFFSET(2)]),
  mozfun.assert.equals(('d', TRUE, 'name', FALSE, TRUE, '', 1, 'type', FALSE, TRUE, 2, 3, 4, TRUE, TRUE, TRUE), result[OFFSET(3)]),
  mozfun.assert.equals(('e', TRUE, 'name', TRUE, TRUE, 'version', 1, 'type', TRUE, TRUE, 2, 3, 4, TRUE, TRUE, TRUE), result[OFFSET(4)]),
  mozfun.assert.equals(('f', FALSE, '', FALSE, FALSE, '', 0, '', FALSE, FALSE, 0, 0, 0, FALSE, FALSE, FALSE), result[OFFSET(5)]),
  mozfun.assert.equals('g', result[OFFSET(6)].addon_id),
  mozfun.assert.all_fields_null((SELECT AS STRUCT result[OFFSET(6)].* EXCEPT (addon_id))),
  mozfun.assert.equals('h', result[OFFSET(7)].addon_id),
  mozfun.assert.all_fields_null((SELECT AS STRUCT result[OFFSET(7)].* EXCEPT (addon_id))),
  mozfun.assert.equals(('i', "version", TRUE, TRUE), STRUCT(result[OFFSET(8)].addon_id, result[OFFSET(8)].version, result[OFFSET(8)].user_disabled, result[OFFSET(8)].foreign_install)),
  mozfun.assert.all_fields_null((SELECT AS STRUCT result[OFFSET(8)].* EXCEPT (addon_id, version, user_disabled, foreign_install))),
  mozfun.assert.equals(('j', "", FALSE, FALSE), STRUCT(result[OFFSET(9)].addon_id, result[OFFSET(9)].version, result[OFFSET(9)].user_disabled, result[OFFSET(9)].foreign_install)),
  mozfun.assert.all_fields_null((SELECT AS STRUCT result[OFFSET(9)].* EXCEPT (addon_id, version, user_disabled, foreign_install))),
  mozfun.assert.equals(('l', TRUE, 'name', TRUE, TRUE, 'version', 1, 'type', TRUE, TRUE, 2, 3, 4, TRUE, TRUE, TRUE), result[OFFSET(10)]),
  mozfun.assert.equals(('m', FALSE, '', FALSE, FALSE, '', 0, '', FALSE, FALSE, 0, 0, 0, FALSE, FALSE, FALSE), result[OFFSET(11)]),
  mozfun.assert.equals('n', result[OFFSET(12)].addon_id),
  mozfun.assert.all_fields_null((SELECT AS STRUCT result[OFFSET(12)].* EXCEPT (addon_id))),
  null
FROM
  result,
  -- use SAFE_OFFSET to get a result with all fields null
  UNNEST([(SELECT AS STRUCT result[SAFE_OFFSET(-1)].* EXCEPT (addon_id))]) AS empty_value
-- format:on
