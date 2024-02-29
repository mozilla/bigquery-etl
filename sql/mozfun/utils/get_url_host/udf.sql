CREATE OR REPLACE FUNCTION utils.get_url_host(url STRING)
RETURNS STRING AS (
  SPLIT(
    SPLIT(REPLACE(REPLACE(REPLACE(url, "android-app://", ""), "http://", ""), "https://", ""), "?")[
      SAFE_OFFSET(0)
    ],
    "/"
  )[SAFE_OFFSET(0)]
);

-- Tests
SELECT
  mozfun.assert.equals("some-url.com", utils.get_url_host("https://some-url.com/path?something")),
  mozfun.assert.equals("some-url.com", utils.get_url_host("http://some-url.com?more")),
  mozfun.assert.equals("some-url.com", utils.get_url_host("android-app://some-url.com?more")),
  mozfun.assert.equals("some-url.com", utils.get_url_host("http://some-url.com")),
