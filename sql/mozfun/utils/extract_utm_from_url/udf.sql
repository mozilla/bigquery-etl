CREATE OR REPLACE FUNCTION utils.extract_utm_from_url(url STRING)
RETURNS STRUCT<
  utm_source STRING,
  utm_medium STRING,
  utm_campaign STRING,
  utm_content STRING,
  utm_term STRING
> AS (
  STRUCT(
    REGEXP_EXTRACT(url, r"[\?&]utm_source=([^&#]*)") AS utm_source,
    REGEXP_EXTRACT(url, r"[\?&]utm_medium=([^&#]*)") AS utm_medium,
    REGEXP_EXTRACT(url, r"[\?&]utm_campaign=([^&#]*)") AS utm_campaign,
    REGEXP_EXTRACT(url, r"[\?&]utm_content=([^&#]*)") AS utm_content,
    REGEXP_EXTRACT(url, r"[\?&]utm_term=([^&#]*)") AS utm_term
  )
);

-- Tests
SELECT
  mozfun.assert.equals("google-rsa", utils.extract_utm_from_url(url_1).utm_source),
  mozfun.assert.equals("paidsearch", utils.extract_utm_from_url(url_1).utm_medium),
  mozfun.assert.equals("fxeu", utils.extract_utm_from_url(url_1).utm_campaign),
  mozfun.assert.equals("A123_A234_7890", utils.extract_utm_from_url(url_1).utm_content),
  mozfun.assert.null(utils.extract_utm_from_url(url_1).utm_term),
  mozfun.assert.equals("bar", utils.extract_utm_from_url(url_2).utm_source),
  mozfun.assert.equals("baz", utils.extract_utm_from_url(url_2).utm_medium),
  mozfun.assert.equals("", utils.extract_utm_from_url(url_2).utm_campaign),
  mozfun.assert.null(utils.extract_utm_from_url(url_2).utm_content),
  mozfun.assert.equals("foo", utils.extract_utm_from_url(url_2).utm_term),
  mozfun.assert.null(utils.extract_utm_from_url(url_3).utm_source),
  mozfun.assert.null(utils.extract_utm_from_url(url_3).utm_medium),
  mozfun.assert.null(utils.extract_utm_from_url(url_3).utm_campaign),
  mozfun.assert.null(utils.extract_utm_from_url(url_3).utm_content),
  mozfun.assert.null(utils.extract_utm_from_url(url_3).utm_term),
  mozfun.assert.null(utils.extract_utm_from_url(url_4).utm_source),
  mozfun.assert.null(utils.extract_utm_from_url(url_4).utm_medium),
  mozfun.assert.null(utils.extract_utm_from_url(url_4).utm_campaign),
  mozfun.assert.null(utils.extract_utm_from_url(url_4).utm_content),
  mozfun.assert.null(utils.extract_utm_from_url(url_4).utm_term),
  mozfun.assert.equals("foo", utils.extract_utm_from_url(url_5).utm_term),
  mozfun.assert.equals("bar'", utils.extract_utm_from_url(url_5).utm_source),
  mozfun.assert.equals("foo", utils.extract_utm_from_url(url_6).utm_term),
  mozfun.assert.equals("bar\"", utils.extract_utm_from_url(url_6).utm_source),
  mozfun.assert.equals("bar]", utils.extract_utm_from_url(url_7).utm_source),
  mozfun.assert.equals("", utils.extract_utm_from_url(url_8).utm_source),
  mozfun.assert.equals("foo", utils.extract_utm_from_url(url_8).utm_campaign)
FROM
  (
    SELECT
      "https://www.mozilla.org/fr/firefox/new/?utm_medium=paidsearch&utm_source=google-rsa&utm_campaign=fxeu&utm_content=A123_A234_7890" AS url_1,
      "http://some-url.com/?utm_term=foo&utm_source=bar&utm_medium=baz&utm_campaign=" AS url_2,
      "https://www.mozilla.org" AS url_3,
      "" AS url_4,
      "'http://some-url.com/?utm_term=foo&utm_source=bar'" AS url_5,
      '"http://some-url.com/?utm_term=foo&utm_source=bar"' AS url_6,
      "[http://some-url.com/?utm_source=bar]" AS url_7,
      "https://some-url.com/?utm_source=&utm_campaign=foo#fragment" AS url_8
  )
