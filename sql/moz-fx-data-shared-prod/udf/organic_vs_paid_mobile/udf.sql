CREATE OR REPLACE FUNCTION udf.organic_vs_paid_mobile(adjust_network STRING)
RETURNS STRING AS (
  CASE
    WHEN LOWER(adjust_network) IN ('google ads aci', 'apple search ads')
      THEN 'Paid'
    ELSE 'Organic'
  END
);

SELECT
  mozfun.assert.equals(udf.organic_vs_paid_mobile('Google Ads ACI'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile('google ads aci'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile('APPLE SEARCH ADS'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile('apple search ads'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile(''), 'Organic'),
  mozfun.assert.equals(udf.organic_vs_paid_mobile(NULL), 'Organic');
