CREATE OR REPLACE FUNCTION google_search_console.extract_url_language_code(url STRING)
RETURNS STRING AS (
  LOWER(SPLIT(google_search_console.extract_url_locale(url), '-')[SAFE_ORDINAL(1)])
);

SELECT
  assert.equals(
    google_search_console.extract_url_language_code('https://www.mozilla.org/en-US/firefox/'),
    'en'
  ),
  assert.equals(
    google_search_console.extract_url_language_code('https://support.mozilla.org/es/'),
    'es'
  ),
  assert.equals(
    google_search_console.extract_url_language_code('https://blog.mozilla.org/ux/'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.extract_url_language_code('https://www.mozilla.org/'),
    CAST(NULL AS STRING)
  ),
