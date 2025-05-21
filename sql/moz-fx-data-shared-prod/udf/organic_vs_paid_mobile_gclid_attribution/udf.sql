CREATE OR REPLACE FUNCTION udf.organic_vs_paid_mobile_gclid_attribution(
  play_store_attribution_install_referrer_response STRING
)
RETURNS STRING AS (
  CASE
    WHEN LOWER(play_store_attribution_install_referrer_response) LIKE "%gclid%"
      THEN 'Paid'
    ELSE 'Organic'
  END
);

SELECT
  mozfun.assert.equals(udf.organic_vs_paid_mobile_gclid_attribution('GCLID-123'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile_gclid_attribution('123-gclid'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile_gclid_attribution(''), 'Organic'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile_gclid_attribution(NULL), 'Organic');
