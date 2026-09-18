/*

Determine whether a Firefox Suggest interaction is for a Mozilla-supplied,
non-AMP, suggestion, based on its reporting_url. These report to the /v1/st
endpoint on ads.mozilla.org (or ads.allizom.org in staging), e.g.

https://ads.mozilla.org/v1/st?suggestion_id=...

Both the host and the path must match, so a Mozilla host on any other path is
not treated as Mozilla-supplied. Returns FALSE rather than NULL for a NULL,
empty or unparseable reporting_url, so callers can use the result directly in a
WHERE clause without COALESCE.

*/
CREATE OR REPLACE FUNCTION udf.is_moz_supplied_suggestion(reporting_url STRING)
RETURNS BOOLEAN AS (
  COALESCE(
    -- NET.REG_DOMAIN preserves the case of the input host, so lowercase it before
    -- comparing; the path pattern below is matched case-insensitively via (?i).
    LOWER(NET.REG_DOMAIN(reporting_url)) IN ('mozilla.org', 'allizom.org')
    AND REGEXP_CONTAINS(reporting_url, r'(?i)^https?://[^/?#]+/v1/st(?:[/?#]|$)'),
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
  -- Host and path matching are case-insensitive.
  mozfun.assert.true(
    udf.is_moz_supplied_suggestion('https://ADS.MOZILLA.ORG/V1/ST?suggestion_id=abc')
  ),
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
