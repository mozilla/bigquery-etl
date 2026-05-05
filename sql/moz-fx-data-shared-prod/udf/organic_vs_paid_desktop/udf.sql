CREATE OR REPLACE FUNCTION udf.organic_vs_paid_desktop(medium STRING)
RETURNS STRING AS (
  CASE
    WHEN LOWER(medium) IN (
        'paidsearch',
        'paiddisplay',
        'cpc',
        'display',
        'paidsocial',
        'paidvideo',
        'ppc'
      )
      THEN 'Paid'
    ELSE 'Organic'
  END
);

SELECT
  mozfun.assert.equals(udf.organic_vs_paid_desktop('display'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('PAIDDISPLAY'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('paidsocial'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('paidvideo'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('ppc'), 'Paid'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop(NULL), 'Organic'),
  mozfun.assert.equals(udf.organic_vs_paid_desktop('abc'), 'Organic');
