CREATE OR REPLACE FUNCTION google_search_console.extract_url_domain_name(url STRING)
RETURNS STRING AS (
  REGEXP_EXTRACT(url, r'^(?:https?://|sc-domain:)([^/]+)')
);

SELECT
  assert.equals(
    google_search_console.extract_url_domain_name('https://www.mozilla.org/'),
    'www.mozilla.org'
  ),
  assert.equals(
    google_search_console.extract_url_domain_name('sc-domain:addons.mozilla.org'),
    'addons.mozilla.org'
  ),
