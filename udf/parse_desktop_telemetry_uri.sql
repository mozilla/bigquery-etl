/*

Parses and labels the components of a telemetry desktop ping submission uri
Per https://docs.telemetry.mozilla.org/concepts/pipeline/http_edge_spec.html#special-handling-for-firefox-desktop-telemetry
the format is /submit/telemetry/docId/docType/appName/appVersion/appUpdateChannel/appBuildID
e.g. /submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648/main/Firefox/61.0a1/nightly/20180328030202

*/
CREATE TEMP FUNCTION udf_parse_desktop_telemetry_uri(uri STRING)
RETURNS STRUCT<
  namespace STRING,
  document_id STRING,
  document_type STRING,
  app_name STRING,
  app_version STRING,
  app_update_channel STRING,
  app_build_id STRING>
AS (
  CASE
    WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")) = 8 THEN
      STRUCT(
        REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[ OFFSET (1)],  --namespace
        REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[ OFFSET (2)],  --document_id
        REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[ OFFSET (3)],  --document_type
        REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[ OFFSET (4)],  --app_name
        REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[ OFFSET (5)],  --app_version
        REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[ OFFSET (6)],  --app_update_channel
        REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[ OFFSET (7)]   --app_build_id
      )
    ELSE NULL
  END
);

-- Tests

SELECT
    assert_equals("telemetry", udf_parse_desktop_telemetry_uri(uri1).namespace),
    assert_equals("ce39b608-f595-4c69-b6a6-f7a436604648", udf_parse_desktop_telemetry_uri(uri1).document_id),
    assert_equals("main", udf_parse_desktop_telemetry_uri(uri1).document_type),
    assert_equals("Firefox", udf_parse_desktop_telemetry_uri(uri1).app_name),
    assert_equals("61.0a1", udf_parse_desktop_telemetry_uri(uri1).app_version),
    assert_equals("nightly", udf_parse_desktop_telemetry_uri(uri1).app_update_channel),
    assert_equals("20180328030202", udf_parse_desktop_telemetry_uri(uri1).app_build_id),
    assert_null(udf_parse_desktop_telemetry_uri(uri2))
FROM (
  SELECT
    "/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648/main/Firefox/61.0a1/nightly/20180328030202" AS uri1,
    "/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648/main/Firefox/61.0a1/20180328030202" AS uri2
);
