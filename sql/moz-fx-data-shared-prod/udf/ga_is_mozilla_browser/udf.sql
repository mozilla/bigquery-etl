/*

Determine if a browser in a Google Analytics data is one produced by Mozilla

*/
CREATE OR REPLACE FUNCTION udf.ga_is_mozilla_browser(browser STRING)
RETURNS BOOLEAN AS (
  CASE
    WHEN browser = 'Firefox'
      OR browser = 'Mozilla'
      THEN TRUE
    ELSE FALSE
  END
);

                -- Tests
SELECT
  mozfun.assert.true(udf.ga_is_mozilla_browser('Mozilla')),
  mozfun.assert.true(udf.ga_is_mozilla_browser('Firefox')),
  mozfun.assert.false(udf.ga_is_mozilla_browser('Chrome'));
