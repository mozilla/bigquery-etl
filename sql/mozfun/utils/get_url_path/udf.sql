CREATE OR REPLACE FUNCTION utils.get_url_path(url STRING)
RETURNS STRING AS (
  "/" || COALESCE(
    REGEXP_EXTRACT(REPLACE(REPLACE(url, "https://", ""), "http://", ""), r"\/([^&?#]*)"),
    ""
  )
);

-- Tests
SELECT
  mozfun.assert.equals("/path", utils.get_url_path("https://some-url.com/path")),
  mozfun.assert.equals("/path", utils.get_url_path("http://some-url.com/path?more")),
  mozfun.assert.equals("/path", utils.get_url_path("http://some-url.com/path#more")),
  mozfun.assert.equals("/path", utils.get_url_path("http://some-url.com/path?more&utm=123")),
  mozfun.assert.equals(
    "/path/with/multiple/slashes",
    utils.get_url_path("http://some-url.com/path/with/multiple/slashes?more")
  ),
  mozfun.assert.equals("/", utils.get_url_path("https://some-url.com")),
  mozfun.assert.equals("/", utils.get_url_path("https://some-url.com/")),
  mozfun.assert.equals("/path", utils.get_url_path("some-url.com/path"))
