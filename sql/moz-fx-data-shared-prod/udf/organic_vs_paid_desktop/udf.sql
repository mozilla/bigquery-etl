CREATE OR REPLACE FUNCTION udf.organic_vs_paid_desktop(medium STRING, adjust_network STRING)
RETURNS STRING AS (
  CASE
    WHEN LOWER(medium) IN (
        'paidsearch',
        'paiddisplay',
        'cpc',
        'email',
        'display',
        'paidsocial',
        'paidvideo',
        'social',
        'ppc',
        'banner',
        'ad-unit',
        'newsletter'
      )
      OR LOWER(adjust_network) IN (
        'product marketing (owned media)',
        'apple search ads',
        'google ads aci',
        'google organic search'
      )
      THEN 'Paid'
    ELSE 'Organic'
  END
);

SELECT
  mozfun.assert.equals(udf.organic_vs_paid_desktop('display', NULL), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('PAIDDISPLAY', NULL), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('paidsocial', NULL), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('ad-unit', NULL), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop(NULL, 'apple search ads'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('abc', 'google ads aci'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop(NULL, NULL), 'Organic'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop(NULL, 'abc'), 'Organic');
