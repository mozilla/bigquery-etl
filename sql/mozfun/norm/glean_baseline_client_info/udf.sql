/*

Accepts a glean client_info struct as input and returns a modified struct that
includes a few parsed or normalized variants of the input fields.

*/
CREATE OR REPLACE FUNCTION norm.glean_baseline_client_info(
  client_info ANY TYPE,
  metrics ANY TYPE
) AS (
  (
    SELECT AS STRUCT
      client_info.* REPLACE (
        COALESCE(client_info.locale, metrics.string.glean_baseline_locale) AS locale
      )
  )
);

-- Tests
SELECT
  assert.equals(
    'en-US',
    norm.glean_baseline_client_info(
      STRUCT('en-US' AS locale),
      STRUCT(STRUCT('en-GB' AS glean_baseline_locale) AS string)
    ).locale
  ),
  assert.equals(
    'en-US',
    norm.glean_baseline_client_info(
      STRUCT('en-US' AS locale),
      STRUCT(STRUCT(CAST(NULL AS STRING) AS glean_baseline_locale) AS string)
    ).locale
  ),
  assert.equals(
    'en-GB',
    norm.glean_baseline_client_info(
      STRUCT(CAST(NULL AS STRING) AS locale),
      STRUCT(STRUCT('en-GB' AS glean_baseline_locale) AS string)
    ).locale
  ),
  assert.null(
    norm.glean_baseline_client_info(
      STRUCT(CAST(NULL AS STRING) AS locale),
      STRUCT(STRUCT(CAST(NULL AS STRING) AS glean_baseline_locale) AS string)
    ).locale
  )
