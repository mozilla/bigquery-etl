CREATE OR REPLACE FUNCTION utils.extract_params_from_url(url STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING, depth INT64>> AS (
  -- Extracts every parameter, not one regex per parameter we want. Unlike
  -- utils.extract_utm_from_url, no fixed key list and no leading '?' required.
  ARRAY(
    SELECT AS STRUCT
      key,
      mozfun.utils.url_decode(value) AS value,
      depth
    FROM
      (
        -- One row per key. Shallowest wins, then first occurrence, so a key
        -- repeated at the same depth still resolves the same way every run.
        SELECT
          key,
          ARRAY_AGG(value ORDER BY depth, off LIMIT 1)[SAFE_OFFSET(0)] AS value,
          MIN(depth) AS depth
        FROM
          (
            -- depth 0 as-is; depth 2 wholly encoded ('clid%3DAbc123...'), which a
            -- depth-0 split misses. Same split over a separator-decoded copy.
            SELECT
              LOWER(TRIM(REGEXP_EXTRACT(kv, r'^([^=]+)'))) AS key,
              REGEXP_EXTRACT(kv, r'^[^=]+=(.*)$') AS value,
              lvl.depth AS depth,
              off
            FROM
              UNNEST(
                [
                  STRUCT(url AS s, 0 AS depth),
                  STRUCT(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(IFNULL(url, ''), r'(?i)%3D', '='),
                      r'(?i)%26',
                      '&'
                    ) AS s,
                    2 AS depth
                  )
                ]
              ) AS lvl,
              UNNEST(REGEXP_EXTRACT_ALL(IFNULL(lvl.s, ''), r'[^?&#]+')) AS kv
              WITH OFFSET AS off
            WHERE
              REGEXP_CONTAINS(kv, r'=')
            UNION ALL
            -- depth 1: a query string nested in one value, the partner preload surface.
            -- Per-value, not whole-string: decoding first loses the leading nested key.
            SELECT
              LOWER(TRIM(REGEXP_EXTRACT(nested_kv, r'^([^=]+)'))) AS key,
              REGEXP_EXTRACT(nested_kv, r'^[^=]+=(.*)$') AS value,
              1 AS depth,
              off
            FROM
              UNNEST(REGEXP_EXTRACT_ALL(IFNULL(url, ''), r'[^?&#]+')) AS kv
              WITH OFFSET AS off,
              UNNEST(
                REGEXP_EXTRACT_ALL(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      IFNULL(REGEXP_EXTRACT(kv, r'^[^=]+=(.*)$'), ''),
                      r'(?i)%3D',
                      '='
                    ),
                    r'(?i)%26',
                    '&'
                  ),
                  r'[^?&#]+'
                )
              ) AS nested_kv
            WHERE
              REGEXP_CONTAINS(kv, r'(?i)%3D|%26')
              -- Require a non-'=' after the separator, so encoded base64
              -- padding ('%3D%3D') does not fabricate a key.
              AND REGEXP_CONTAINS(nested_kv, r'^[^=]+=[^=]')
            UNION ALL
            -- depth 0, keyless: some referrers are a bare click id with no 'key='
            -- at all. The token is the signal, so it is emitted as the key with a
            -- NULL value; test for it with a key match, not mozfun.map.get_key.
            -- An underscore is required so a bare package name or word stays unparsed.
            SELECT
              LOWER(TRIM(kv)) AS key,
              CAST(NULL AS STRING) AS value,
              0 AS depth,
              off
            FROM
              UNNEST(REGEXP_EXTRACT_ALL(IFNULL(url, ''), r'[^?&#]+')) AS kv
              WITH OFFSET AS off
            WHERE
              NOT REGEXP_CONTAINS(kv, r'=')
              AND REGEXP_CONTAINS(kv, r'_')
          )
        WHERE
          -- Drop junk keys from splitting a non-query-string. A keyless token is the
          -- signal itself, so it is kept whole and the 60-char cap does not apply.
          REGEXP_CONTAINS(key, r'^[a-z0-9_.\-]{1,60}$')
          OR (value IS NULL AND REGEXP_CONTAINS(key, r'^[a-z0-9_.\-]{2,300}$'))
        GROUP BY
          key
      )
    ORDER BY
      depth,
      key
  )
);

-- Tests
SELECT
  -- depth 0. Note the first parameter is found without a leading '?' or '&'.
  mozfun.assert.map_equals(
    [STRUCT('utm_source' AS key, 'example-store' AS value), ('utm_medium', 'organic')],
    utils.extract_params_from_url('utm_source=example-store&utm_medium=organic')
  ),
  -- a bare click id with no 'key=' becomes a key with a NULL value
  mozfun.assert.equals(
    'vendor_c_i_abc123',
    (SELECT p.key FROM UNNEST(utils.extract_params_from_url('vendor_c_i_abc123')) AS p)
  ),
  mozfun.assert.null(
    (SELECT p.value FROM UNNEST(utils.extract_params_from_url('vendor_c_i_abc123')) AS p)
  ),
  -- depth 1: the partner preload shape
  mozfun.assert.equals(
    'partner-preinstall',
    mozfun.map.get_key(
      utils.extract_params_from_url(
        'utm_source=partner-installs&utm_medium=preload&utm_campaign=tracker_id%3Dtrk123%26partner_click_id%3D1%26partner_campaign%3Dpartner-preinstall'
      ),
      'partner_campaign'
    )
  ),
  mozfun.assert.equals(
    'trk123',
    mozfun.map.get_key(
      utils.extract_params_from_url(
        'utm_source=partner-installs&utm_medium=preload&utm_campaign=tracker_id%3Dtrk123%26partner_click_id%3D1%26partner_campaign%3Dpartner-preinstall'
      ),
      'tracker_id'
    )
  ),
  -- the container survives, decoded, rather than being filtered away
  mozfun.assert.equals(
    'tracker_id=trk123&partner_click_id=1&partner_campaign=partner-preinstall',
    mozfun.map.get_key(
      utils.extract_params_from_url(
        'utm_source=partner-installs&utm_medium=preload&utm_campaign=tracker_id%3Dtrk123%26partner_click_id%3D1%26partner_campaign%3Dpartner-preinstall'
      ),
      'utm_campaign'
    )
  ),
  -- depth 2: whole string encoded, no literal '=' anywhere
  mozfun.assert.equals(
    'example.com',
    mozfun.map.get_key(
      utils.extract_params_from_url(
        'utm_source%3Dexample.com%26utm_medium%3Dcontent%26utm_campaign%3Ddownload'
      ),
      'utm_source'
    )
  ),
  mozfun.assert.equals(
    'c_i_abc123',
    mozfun.map.get_key(
      utils.extract_params_from_url('external_click_id%3Dc_i_abc123'),
      'external_click_id'
    )
  ),
  -- ad_campaign_id, which the partner campaign id leaves unidentified
  mozfun.assert.equals(
    '123456',
    mozfun.map.get_key(
      utils.extract_params_from_url('utm_source=vendor_a&ad_campaign_id=123456&ad_source=1'),
      'ad_campaign_id'
    )
  ),
  -- the shallowest occurrence of a key wins, so get_key is deterministic
  mozfun.assert.equals(
    'shallow',
    mozfun.map.get_key(
      utils.extract_params_from_url('clid=shallow&utm_campaign=clid%3Ddeep'),
      'clid'
    )
  ),
  -- percent-encoded values are decoded, so one campaign is one campaign
  mozfun.assert.equals(
    'Summer Promo',
    mozfun.map.get_key(utils.extract_params_from_url('utm_campaign=Summer%20Promo'), 'utm_campaign')
  ),
  -- split on the first '=' only, so base64 padding survives and is not re-split
  mozfun.assert.equals(
    'c_i_abc==',
    mozfun.map.get_key(
      utils.extract_params_from_url('external_click_id=c_i_abc=='),
      'external_click_id'
    )
  ),
  mozfun.assert.equals(
    1,
    ARRAY_LENGTH(utils.extract_params_from_url('external_click_id=c_i_abc=='))
  ),
  -- full-URL form: the scheme/host token carries no '=' and is dropped
  mozfun.assert.equals(
    2,
    ARRAY_LENGTH(utils.extract_params_from_url('https://example.com/store?utm_source=x&utm_term=y'))
  ),
  -- an unsubstituted ad-server macro is a value like any other, not a crash
  mozfun.assert.equals(
    '{clid}',
    mozfun.map.get_key(utils.extract_params_from_url('clid%3D%7Bclid%7D'), 'clid')
  ),
  -- encoded base64 padding is not a nested pair, so no key is fabricated
  mozfun.assert.equals(1, ARRAY_LENGTH(utils.extract_params_from_url('clid=YWJjZA%3D%3D'))),
  mozfun.assert.equals(
    'YWJjZA==',
    mozfun.map.get_key(utils.extract_params_from_url('clid=YWJjZA%3D%3D'), 'clid')
  ),
  -- a fragment ends the preceding value rather than being absorbed into it
  mozfun.assert.equals(
    'y',
    mozfun.map.get_key(
      utils.extract_params_from_url('https://example.com/store?utm_source=x&utm_term=y#section'),
      'utm_term'
    )
  ),
  -- parameters after a fragment are still returned
  mozfun.assert.equals(
    'abc',
    mozfun.map.get_key(utils.extract_params_from_url('utm_source=x&ie=utf-8#sbfbu=1&pi=abc'), 'pi')
  ),
  -- a key repeated at the same depth resolves to the first occurrence
  mozfun.assert.equals(
    'a',
    mozfun.map.get_key(utils.extract_params_from_url('utm_source=a&utm_source=b'), 'utm_source')
  ),
  -- single-character keys are kept
  mozfun.assert.equals(
    '1',
    mozfun.map.get_key(utils.extract_params_from_url('q=1&utm_source=x'), 'q')
  ),
  -- bare tokens are not query strings and correctly yield nothing
  -- a keyless token is only kept when it carries an underscore, so a bare package
  -- name is still not a parameter
  mozfun.assert.equals(0, ARRAY_LENGTH(utils.extract_params_from_url('com.example.app'))),
  mozfun.assert.equals(0, ARRAY_LENGTH(utils.extract_params_from_url('referral'))),
  mozfun.assert.equals(0, ARRAY_LENGTH(utils.extract_params_from_url(''))),
  mozfun.assert.equals(0, ARRAY_LENGTH(utils.extract_params_from_url(NULL)));
