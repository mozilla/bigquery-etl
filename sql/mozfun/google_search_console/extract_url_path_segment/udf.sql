CREATE OR REPLACE FUNCTION google_search_console.extract_url_path_segment(
  url STRING,
  segment_number INTEGER
)
RETURNS STRING AS (
  NULLIF(SPLIT(google_search_console.extract_url_path(url), '/')[SAFE_OFFSET(segment_number)], '')
);

SELECT
  assert.equals(
    google_search_console.extract_url_path_segment('https://www.mozilla.org/en-US/firefox/', 1),
    'en-US'
  ),
  assert.equals(
    google_search_console.extract_url_path_segment('https://www.mozilla.org/en-US/firefox/', 2),
    'firefox'
  ),
  assert.equals(
    google_search_console.extract_url_path_segment('https://www.mozilla.org/en-US/firefox/', 3),
    CAST(NULL AS STRING)
  ),
