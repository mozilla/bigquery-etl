CREATE OR REPLACE FUNCTION udf.marketing_attributable_desktop(medium STRING)
RETURNS BOOLEAN AS (
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
      THEN TRUE
    ELSE FALSE
  END
);
