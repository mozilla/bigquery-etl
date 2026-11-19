CREATE OR REPLACE FUNCTION utils.url_decode(s STRING) AS (
  IFNULL(
    (
      SELECT
        -- Reassemble in original order; UNNEST does not preserve it.
        STRING_AGG(
          IF(
            -- Length guard: a lone '%' is not an escape.
            STARTS_WITH(tok, '%')
            AND LENGTH(tok) > 1,
            -- '%C3%A9' -> 'C3A9' -> bytes -> 'é'.
            -- Bad bytes give U+FFFD. SAFE_CAST would NULL, and
            -- STRING_AGG skips NULLs, dropping the token silently.
            SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(tok, '%', ''))),
            tok
          ),
          ''
          ORDER BY
            off
        )
      FROM
        -- Three token types: escape run, stray '%', plain text.
        -- Runs stay whole so multi-byte UTF-8 decodes as one unit.
        -- '+' is plain text, so never becomes a space.
        UNNEST(REGEXP_EXTRACT_ALL(s, r'(?:%[0-9a-fA-F]{2})+|%|[^%]+')) AS tok
        WITH OFFSET AS off
    ),
    -- STRING_AGG over no rows is NULL, so '' and NULL both fall through here.
    s
  )
);

-- Tests
SELECT
  mozfun.assert.null(utils.url_decode(NULL)),
  mozfun.assert.equals('', utils.url_decode('')),
  mozfun.assert.equals('plain_token', utils.url_decode('plain_token')),
  -- one campaign arriving as two spellings is the reason this exists
  mozfun.assert.equals('Summer Promo', utils.url_decode('Summer%20Promo')),
  mozfun.assert.equals('a b', utils.url_decode('a%20b')),
  -- lowercase escapes decode too
  mozfun.assert.equals('a+b', utils.url_decode('a%2bb')),
  -- multi-byte UTF-8 survives: the run decodes as one unit, not per byte
  mozfun.assert.equals('café', utils.url_decode('caf%C3%A9')),
  -- adjacent escapes are one run and still decode individually
  mozfun.assert.equals('=&', utils.url_decode('%3D%26')),
  -- '+' is not decoded to a space: base64 click ids carry a literal '+'
  mozfun.assert.equals('c_i_a+b', utils.url_decode('c_i_a+b')),
  -- a stray '%' is not an escape and is kept rather than dropped
  mozfun.assert.equals('100%', utils.url_decode('100%')),
  mozfun.assert.equals('50%off', utils.url_decode('50%off')),
  mozfun.assert.equals('%', utils.url_decode('%')),
  -- too few digits, or non-hex digits, is not an escape either
  mozfun.assert.equals('%2', utils.url_decode('%2')),
  mozfun.assert.equals('%GG', utils.url_decode('%GG')),
  -- one pass only, so an encoded '%' is not decoded twice
  mozfun.assert.equals('a%20b', utils.url_decode('a%2520b')),
  -- a truncated sequence gives U+FFFD, making the loss visible not silent
  mozfun.assert.equals('caf�x', utils.url_decode('caf%C3x')),
  -- separators decode like any other escape
  mozfun.assert.equals(
    'tracker_id=abc&partner_campaign=partner-preinstall',
    utils.url_decode('tracker_id%3Dabc%26partner_campaign%3Dpartner-preinstall')
  );
