/*
The function parses the `payload` column in `moz-fx-data-shared-prod.payload_bytes_error.contextual_services`
to extract the `source` column
*/
CREATE OR REPLACE FUNCTION udf_js.parse_sponsored_interaction(params STRING)
RETURNS STRUCT<
  `source` STRING,
  formFactor STRING,
  scenario STRING,
  interactionType STRING,
  contextId STRING,
  reportingUrl STRING,
  requestId STRING,
  submissionTimestamp TIMESTAMP,
  parsedReportingUrl JSON,
  originalDocType STRING,
  originalNamespace STRING,
  interactionCount INTEGER,
  flaggedFraud BOOLEAN
>
LANGUAGE js
AS
  """
  const parseURL = (url) => {
    const [base, params] = url.split("?");
    const [scheme, _, host, ...path] = base.split("/")
    return {
      scheme,
      host,
      path: path.join("/"),
      params: params.split("&").map(kv => kv.split("="))
        .reduce((acc, [key, value]) => { acc[key.replace("-", "_")] = value; return acc }, {}),
    };
  }

  if (!params) {
    return {};
  }

  try {
    let interaction = params.split(',')
      .map((kv) => kv.split(/=(.*)/s))
      .reduce((acc, [key, value]) => {
        key = key.trim("\\n");
        if (key == "submissionTimestamp") {
          value = new Date(value);
        }
        if (key == "reportingUrl") {
          acc["parsedReportingUrl"] = parseURL(value.repeat(1));
        }
        acc[key] = value;
        return acc;
      }, {});


    if (interaction.parsedReportingUrl?.params?.impressions) {
      interaction.source = "topsites";
      interaction.formFactor = interaction.parsedReportingUrl.params.form_factor;
      interaction.interactionType = "impression";
    }

    if (interaction.interactionType == "impression" && interaction.source == "topsites") {
      interaction.interactionCount = Number(interaction.parsedReportingUrl.params.impressions)
    } else {
      interaction.interactionCount = 1;
    }

    interaction.flaggedFraud = (
      interaction.reportingUrl.includes("click-status=65")
      || interaction.reportingUrl.includes("custom-data=invalid"));

    return interaction;
  }
  catch(err) {
    return {source: err.message}
  }
""";

WITH events AS (
  SELECT AS VALUE
    "source\u003dtopsites, formFactor\u003ddesktop, scenario\u003dnull, interactionType\u003dclick, contextId\u003d{10679079-b1cd-45a3-9e40-cdfb364d3476}, reportingUrl\u003dhttps://bridge.sfo1.ap01.net/ctp?ci\u003d1681139740815.12791\u0026country-code\u003dDE\u0026ctag\u003dpd_sl_08aeb79c14ac3da0f8e9116cdcb0afadec2e24da616da802ba033bf6\u0026dma-code\u003d\u0026form-factor\u003ddesktop\u0026key\u003d1681139740400900002.1\u0026os-family\u003dWindows\u0026product-version\u003dfirefox_111\u0026region-code\u003dNW\u0026version\u003d16.0.0, requestId\u003dnull, submissionTimestamp\u003d2023-04-10T15:41:55Z, originalDocType\u003dtopsites-click, originalNamespace\u003dcontextual-services"
),
extracted AS (
  SELECT
    udf_js.parse_sponsored_interaction(events) AS e
  FROM
    events
)
SELECT
  assert.equals("topsites", e.`source`),
  assert.equals("desktop", e.formFactor),
  assert.null(e.scenario),
  assert.equals("click", e.interactionType),
  assert.equals("{10679079-b1cd-45a3-9e40-cdfb364d3476}", e.contextId),
  assert.equals(
    "https://bridge.sfo1.ap01.net/ctp?ci=1681139740815.12791&country-code=DE&ctag=pd_sl_08aeb79c14ac3da0f8e9116cdcb0afadec2e24da616da802ba033bf6&dma-code=&form-factor=desktop&key=1681139740400900002.1&os-family=Windows&product-version=firefox_111&region-code=NW&version=16.0.0",
    e.reportingUrl
  ),
  assert.null(e.requestId),
  assert.equals(TIMESTAMP("2023-04-10 15:41:55 UTC"), e.submissionTimestamp),
  assert.equals("topsites-click", e.originalDocType),
  assert.equals("contextual-services", e.originalNamespace),
  assert.equals(1, e.interactionCount),
  assert.false(e.flaggedFraud),
  assert.equals(
    '{\"host\":\"bridge.sfo1.ap01.net\",\"params\":{\"ci\":\"1681139740815.12791\",\"country_code\":\"DE\",\"ctag\":\"pd_sl_08aeb79c14ac3da0f8e9116cdcb0afadec2e24da616da802ba033bf6\",\"dma_code\":\"\",\"form_factor\":\"desktop\",\"key\":\"1681139740400900002.1\",\"os_family\":\"Windows\",\"product_version\":\"firefox_111\",\"region_code\":\"NW\",\"version\":\"16.0.0\"},\"path\":\"ctp\",\"scheme\":\"https:\"}',
    TO_JSON_STRING(e.parsedReportingUrl)
  )
FROM
  extracted
