/*

Parses and labels the components of a telemetry desktop ping submission uri
Per https://docs.telemetry.mozilla.org/concepts/pipeline/http_edge_spec.html#special-handling-for-firefox-desktop-telemetry
the format is /submit/telemetry/docId/docType/appName/appVersion/appUpdateChannel/appBuildID
e.g. /submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648/main/Firefox/61.0a1/nightly/20180328030202

*/
CREATE OR REPLACE FUNCTION udf.parse_desktop_telemetry_uri(uri STRING)
RETURNS STRUCT<
  namespace STRING,
  document_id STRING,
  document_type STRING,
  app_name STRING,
  app_version STRING,
  app_update_channel STRING,
  app_build_id STRING
> AS (
  CASE
    WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")) = 8
      THEN STRUCT(
          REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[OFFSET(1)],  --namespace
          REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[OFFSET(2)],  --document_id
          REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[OFFSET(3)],  --document_type
          REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[OFFSET(4)],  --app_name
          REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[OFFSET(5)],  --app_version
          REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[OFFSET(6)],  --app_update_channel
          REGEXP_EXTRACT_ALL(uri, r"/([a-zA-Z0-9_.+-]+)")[OFFSET(7)]   --app_build_id
        )
    ELSE NULL
  END
);

-- Tests
SELECT
  mozfun.assert.equals("telemetry", udf.parse_desktop_telemetry_uri(uri1).namespace),
  mozfun.assert.equals(
    "ce39b608-f595-4c69-b6a6-f7a436604648",
    udf.parse_desktop_telemetry_uri(uri1).document_id
  ),
  mozfun.assert.equals("main", udf.parse_desktop_telemetry_uri(uri1).document_type),
  mozfun.assert.equals("Firefox", udf.parse_desktop_telemetry_uri(uri1).app_name),
  mozfun.assert.equals("61.0a1", udf.parse_desktop_telemetry_uri(uri1).app_version),
  mozfun.assert.equals("nightly", udf.parse_desktop_telemetry_uri(uri1).app_update_channel),
  mozfun.assert.equals("20180328030202", udf.parse_desktop_telemetry_uri(uri1).app_build_id),
  mozfun.assert.null(udf.parse_desktop_telemetry_uri(uri2))
FROM
  (
    SELECT
      "/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648/main/Firefox/61.0a1/nightly/20180328030202" AS uri1,
      "/submit/telemetry/ce39b608-f595-4c69-b6a6-f7a436604648/main/Firefox/61.0a1/20180328030202" AS uri2
  );
