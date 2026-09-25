/*

Determine whether a Firefox Suggest interaction is for a Mozilla-supplied,
non-AMP suggestion, based on its reporting_url. These report to the /v1/st
endpoint on ads.mozilla.org (or ads.allizom.org in staging), e.g.

https://ads.mozilla.org/v1/st?suggestion_id=...

Both the host and the path must match exactly, so neither another host under
mozilla.org nor another path on ads.mozilla.org is treated as Mozilla-supplied.

That makes this fail open: a reporting_url format we do not recognise is
reported as not-Mozilla-supplied. The checks.sql on
search_terms_derived.suggest_impression_sanitized_v3 is the backstop -- it fails
the DAG when a mozilla.org/allizom.org reporting_url appears that this function
does not match.

Returns FALSE rather than NULL for a NULL, empty or unparseable reporting_url,
so callers can use the result directly in a WHERE clause without COALESCE.

*/
CREATE OR REPLACE FUNCTION udf.is_moz_supplied_suggestion(reporting_url STRING)
RETURNS BOOLEAN AS (
  COALESCE(
    -- NET.HOST parses the host whether or not a scheme is present, but preserves
    -- its case, so lowercase it before comparing.
    LOWER(NET.HOST(reporting_url)) IN ('ads.mozilla.org', 'ads.allizom.org')
    -- The regex checks the path, part by part:
    --   (?i)            match case-insensitively
    --   ^               anchor at the start of reporting_url
    --   (?:[^/?#]*//)?  optional "scheme://" prefix, absent on a bare host
    --   [^/?#]*         the host; cannot contain / ? or #, so /v1/st below has
    --                   to be the first path segment, not /something/v1/st
    --   /v1/st          the literal endpoint path
    --   (?:[/?#]|$)     boundary: end of string, or a sub-path, query string or
    --                   fragment follows. /v1/st, /v1/st/extra and /v1/st?a=b
    --                   all match; /v1/status does not
    AND REGEXP_CONTAINS(reporting_url, r'(?i)^(?:[^/?#]*//)?[^/?#]*/v1/st(?:[/?#]|$)'),
    FALSE
  )
);

-- Tests
SELECT
  -- Mozilla-supplied: production and staging hosts on the /v1/st endpoint.
  mozfun.assert.true(
    udf.is_moz_supplied_suggestion('https://ads.mozilla.org/v1/st?suggestion_id=abc123')
  ),
  mozfun.assert.true(
    udf.is_moz_supplied_suggestion('https://ads.allizom.org/v1/st?suggestion_id=abc123')
  ),
  mozfun.assert.true(udf.is_moz_supplied_suggestion('https://ads.mozilla.org/v1/st')),
  mozfun.assert.true(udf.is_moz_supplied_suggestion('https://ads.mozilla.org/v1/st/extra')),
  -- Host and path matching are case-insensitive, and the scheme is optional.
  mozfun.assert.true(
    udf.is_moz_supplied_suggestion('https://ADS.MOZILLA.ORG/V1/ST?suggestion_id=abc')
  ),
  mozfun.assert.true(udf.is_moz_supplied_suggestion('ads.mozilla.org/v1/st?suggestion_id=abc')),
  -- Only these two hosts count; another host under the same domain does not.
  mozfun.assert.false(udf.is_moz_supplied_suggestion('https://www.mozilla.org/v1/st')),
  mozfun.assert.false(udf.is_moz_supplied_suggestion('https://ads.stage.allizom.org/v1/st')),
  -- AMP hosts: impressions report to mt48.net, clicks to admarketplace.net.
  mozfun.assert.false(udf.is_moz_supplied_suggestion('https://imp.mt48.net/imp?id=abc')),
  mozfun.assert.false(
    udf.is_moz_supplied_suggestion('https://bridge.pdx1.admarketplace.net/ctp?ci=abc')
  ),
  -- A Mozilla host on any other path is not Mozilla-supplied.
  mozfun.assert.false(
    udf.is_moz_supplied_suggestion('https://ads.mozilla.org/v1/status?suggestion_id=abc')
  ),
  mozfun.assert.false(
    udf.is_moz_supplied_suggestion('https://ads.mozilla.org/v2/st?suggestion_id=abc')
  ),
  mozfun.assert.false(udf.is_moz_supplied_suggestion('https://ads.mozilla.org/')),
  -- A non-Mozilla host on the /v1/st path is not Mozilla-supplied.
  mozfun.assert.false(udf.is_moz_supplied_suggestion('https://imp.mt48.net/v1/st?id=abc')),
  -- Missing or unparseable values are not Mozilla-supplied.
  mozfun.assert.false(udf.is_moz_supplied_suggestion(NULL)),
  mozfun.assert.false(udf.is_moz_supplied_suggestion('')),
  mozfun.assert.false(udf.is_moz_supplied_suggestion('not a url'));
