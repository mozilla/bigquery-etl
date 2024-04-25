CREATE OR REPLACE FUNCTION google_search_console.extract_url_path(url STRING)
RETURNS STRING AS (
  REGEXP_EXTRACT(url, r'^https?://(?:[^/]+)([^\?#]*)')
);

SELECT
  assert.equals(google_search_console.extract_url_path('https://www.mozilla.org'), ''),
  assert.equals(google_search_console.extract_url_path('https://www.mozilla.org/'), '/'),
  assert.equals(
    google_search_console.extract_url_path('https://www.mozilla.org/en-US/firefox/'),
    '/en-US/firefox/'
  ),
  assert.equals(
    google_search_console.extract_url_path('https://www.mozilla.org/en-US/firefox/?foo'),
    '/en-US/firefox/'
  ),
  assert.equals(
    google_search_console.extract_url_path('https://www.mozilla.org/en-US/firefox/#foo'),
    '/en-US/firefox/'
  ),
