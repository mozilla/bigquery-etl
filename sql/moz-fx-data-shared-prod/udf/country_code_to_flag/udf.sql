/*

For a given two-letter ISO 3166-1 alpha-2 country code, returns a string
consisting of two Unicode regional indicator symbols, which is rendered in
supporting fonts (such as in the BigQuery console or STMO) as flag emoji.

This is just for fun.

See:

- https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
- https://en.wikipedia.org/wiki/Regional_Indicator_Symbol

*/
CREATE OR REPLACE FUNCTION udf.country_code_to_flag(
  country_code string
) AS ( --
  CODE_POINTS_TO_STRING(
    ARRAY(
      SELECT
        -- This constant offset transforms from ASCII characters A-Z to
        -- the equivalent Unicode regional indicator symbols A-Z.
        -- See https://en.wikipedia.org/wiki/Regional_Indicator_Symbol
        c + 127397
      FROM
        UNNEST(TO_CODE_POINTS(country_code)) c
    )
  )
);

-- Tests
SELECT
  -- The emoji below may not be visible in your code editor.
  mozfun.assert.equals('🇫🇮', udf.country_code_to_flag('FI'));
