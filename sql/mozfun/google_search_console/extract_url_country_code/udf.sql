CREATE OR REPLACE FUNCTION google_search_console.extract_url_country_code(url STRING)
RETURNS STRING AS (
  UPPER(SPLIT(google_search_console.extract_url_locale(url), '-')[SAFE_ORDINAL(2)])
);

SELECT
  assert.equals(
    google_search_console.extract_url_country_code('https://www.mozilla.org/en-US/firefox/'),
    'US'
  ),
  assert.equals(
    google_search_console.extract_url_country_code('https://www.mozilla.org/en-us/firefox/'),
    'US'
  ),
  assert.equals(
    google_search_console.extract_url_country_code('https://support.mozilla.org/es/'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.extract_url_country_code('https://blog.mozilla.org/ux/'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.extract_url_country_code('https://www.mozilla.org/'),
    CAST(NULL AS STRING)
  ),
