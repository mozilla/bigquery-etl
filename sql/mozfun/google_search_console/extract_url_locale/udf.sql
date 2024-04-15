CREATE OR REPLACE FUNCTION google_search_console.extract_url_locale(url STRING)
RETURNS STRING AS (
  IF(
    REGEXP_CONTAINS(
      google_search_console.extract_url_path_segment(url, 1),
      r'^[a-zA-Z]{2}-[a-zA-Z]{2}$'
    )
    OR LOWER(google_search_console.extract_url_path_segment(url, 1)) IN (
      SELECT
        code_2
      FROM
        `moz-fx-data-shared-prod.static.language_codes_v1`
      WHERE
        code_2 IS NOT NULL
    ),
    google_search_console.extract_url_path_segment(url, 1),
    NULL
  )
);

SELECT
  assert.equals(
    google_search_console.extract_url_locale('https://www.mozilla.org/en-US/firefox/'),
    'en-US'
  ),
  assert.equals(
    google_search_console.extract_url_locale('https://www.mozilla.org/en-us/firefox/'),
    'en-us'
  ),
  assert.equals(google_search_console.extract_url_locale('https://support.mozilla.org/es/'), 'es'),
  assert.equals(
    google_search_console.extract_url_locale('https://blog.mozilla.org/ux/'),
    CAST(NULL AS STRING)
  ),
  assert.equals(
    google_search_console.extract_url_locale('https://www.mozilla.org/'),
    CAST(NULL AS STRING)
  ),
