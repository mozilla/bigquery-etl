-- Definition for udf_js.decode_uri_attribution
-- For more information on writing UDFs see:
-- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
CREATE OR REPLACE FUNCTION udf_js.decode_uri_attribution(attribution STRING)
RETURNS STRUCT<
  campaign STRING,
  content STRING,
  dlsource STRING,
  dltoken STRING,
  experiment STRING,
  medium STRING,
  source STRING,
  ua STRING,
  variation STRING
> DETERMINISTIC
LANGUAGE js
AS
  """
 if (attribution == null) {
  return {};
 }
const columns = [
  'campaign',
  'content',
  'dlsource',
  'dltoken',
  'experiment',
  'medium',
  'source',
  'ua',
  'variation',
];
try { 
return decodeURIComponent(decodeURIComponent(attribution))
  .split('&')
  .map((kv) => kv.split(/=(.*)/s))
  .reduce((acc, [key, value]) => {
    key = key.trim('\\n');
    if (columns.includes(key)) {
      acc[key] = value.trim().replace('not+set', 'not set');
    }
    return acc;
  }, {});
} 
catch {
  return {};
}
""";

-- Tests
-- complete string
SELECT
  mozfun.assert.struct_equals(
    STRUCT(
      'whatsnew',
      '(not set)',
      'mozorg',
      'test-download-token',
      '(not set)',
      'firefox-browser',
      'firefox-browser',
      'firefox',
      '(not set)'
    ),
    udf_js.decode_uri_attribution(
      "campaign%3Dwhatsnew%26content%3D%2528not%2Bset%2529%26dlsource%3Dmozorg%26dltoken%3Dtest-download-token%26experiment%3D%2528not%2Bset%2529%26medium%3Dfirefox-browser%26source%3Dfirefox-browser%26ua%3Dfirefox%26variation%3D%2528not%2Bset%2529"
    )
  );

-- missing fields, need to include schema whenever using NULL
SELECT
  mozfun.assert.struct_equals(
    STRUCT<
      campaign STRING,
      content STRING,
      dlsource STRING,
      dltoken STRING,
      experiment STRING,
      medium STRING,
      source STRING,
      ua STRING,
      variation STRING
    >(
      'whatsnew',
      '(not set)',
      'mozorg',
      'test-download-token',
      NULL,
      'firefox-browser',
      'firefox-browser',
      'firefox',
      NULL
    ),
    udf_js.decode_uri_attribution(
      "campaign%3Dwhatsnew%26content%3D%2528not%2Bset%2529%26dlsource%3Dmozorg%26dltoken%3Dtest-download-token%26medium%3Dfirefox-browser%26source%3Dfirefox-browser%26ua%3Dfirefox"
    )
  );

-- extra fields, need to include schema whenever using NULL
SELECT
  mozfun.assert.struct_equals(
    STRUCT(
      'whatsnew',
      '(not set)',
      'mozorg',
      'test-download-token',
      '(not set)',
      'firefox-browser',
      'firefox-browser',
      'firefox',
      '(not set)'
    ),
    udf_js.decode_uri_attribution(
      "extra%3Dsomething%26campaign%3Dwhatsnew%26content%3D%2528not%2Bset%2529%26dlsource%3Dmozorg%26dltoken%3Dtest-download-token%26experiment%3D%2528not%2Bset%2529%26medium%3Dfirefox-browser%26source%3Dfirefox-browser%26ua%3Dfirefox%26variation%3D%2528not%2Bset%2529"
    )
  );

-- NULL, need to include schema whenever using NULL
SELECT
  mozfun.assert.struct_equals(
    STRUCT<
      campaign STRING,
      content STRING,
      dlsource STRING,
      dltoken STRING,
      experiment STRING,
      medium STRING,
      source STRING,
      ua STRING,
      variation STRING
    >(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    udf_js.decode_uri_attribution(NULL)
  );

-- empty string, need to include schema whenever using NULL
SELECT
  mozfun.assert.struct_equals(
    STRUCT<
      campaign STRING,
      content STRING,
      dlsource STRING,
      dltoken STRING,
      experiment STRING,
      medium STRING,
      source STRING,
      ua STRING,
      variation STRING
    >(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    udf_js.decode_uri_attribution('')
  );

-- not encoded
SELECT
  mozfun.assert.struct_equals(
    STRUCT(
      'whatsnew',
      '(not set)',
      'mozorg',
      'test-download-token',
      '(not set)',
      'firefox-browser',
      'firefox-browser',
      'firefox',
      '(not set)'
    ),
    udf_js.decode_uri_attribution(
      "campaign=whatsnew&content=(not set)&dlsource=mozorg&dltoken=test-download-token&experiment=(not set)&medium=firefox-browser&source=firefox-browser&ua=firefox&variation=(not set)"
    )
  );
