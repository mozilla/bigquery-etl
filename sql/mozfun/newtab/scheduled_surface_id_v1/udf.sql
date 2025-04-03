
-- The value returned by newtab.scheduled_surface_id_v1 represents the Content teams reference for the
-- surface where an article is published on the Newtab.
-- This UDF is composed based on the merino backend implementation here:
-- https://github.com/mozilla-services/merino-py/blob/main/merino/curated_recommendations/provider.py#L66-L103
CREATE OR REPLACE FUNCTION newtab.scheduled_surface_id_v1(country STRING, locale STRING)
RETURNS STRING AS (
  CASE
    WHEN LOWER(SPLIT(locale, '-')[SAFE_OFFSET(0)]) = 'de'
      THEN 'NEW_TAB_DE_DE'
    WHEN LOWER(SPLIT(locale, '-')[SAFE_OFFSET(0)]) = 'es'
      THEN 'NEW_TAB_ES_ES'
    WHEN LOWER(SPLIT(locale, '-')[SAFE_OFFSET(0)]) = 'fr'
      THEN 'NEW_TAB_FR_FR'
    WHEN LOWER(SPLIT(locale, '-')[SAFE_OFFSET(0)]) = 'it'
      THEN 'NEW_TAB_IT_IT'
    WHEN UPPER(country) IN ('US', 'CA')
      THEN 'NEW_TAB_EN_US'
    WHEN UPPER(country) IN ('GB', 'IE')
      THEN 'NEW_TAB_EN_GB'
    WHEN UPPER(country) IN ('IN')
      THEN 'NEW_TAB_EN_INTL'
    WHEN LOWER(SPLIT(locale, '-')[SAFE_OFFSET(0)]) = 'en'
      AND (
        UPPER(SPLIT(locale, '-')[SAFE_OFFSET(1)]) IN ('GB', 'IE')
        OR UPPER(country) IN ('GB', 'IE')
      )
      THEN 'NEW_TAB_EN_GB'
    WHEN LOWER(SPLIT(locale, '-')[SAFE_OFFSET(0)]) = 'en'
      AND (UPPER(SPLIT(locale, '-')[SAFE_OFFSET(1)]) = 'IN' OR UPPER(country) = 'IN')
      THEN 'NEW_TAB_EN_INTL'
    WHEN LOWER(SPLIT(locale, '-')[SAFE_OFFSET(0)]) = 'en'
      AND (
        UPPER(SPLIT(locale, '-')[SAFE_OFFSET(1)]) IN ('US', 'CA')
        OR UPPER(country) IN ('US', 'CA')
      )
      THEN 'NEW_TAB_EN_US'
    ELSE 'NEW_TAB_EN_US'
  END
);

-- Tests
SELECT
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('US', 'en-US')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('IE', 'en-IE')),
  assert.equals('NEW_TAB_EN_INTL', newtab.scheduled_surface_id_v1('IN', 'en-US')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('CA', 'en-US')),
  -- Any other country - locale combination of country-locale will be classified as NEW_TAB_EN_US, including NULL values.
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('DE', 'arch')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('DE', NULL)),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1(NULL, NULL)) assert.equals(
    'NEW_TAB_EN_US',
    newtab.scheduled_surface_id_v1('US', 'en-CA')
  ),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('US', 'en-GB')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('US', 'en-US')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('CA', 'en-CA')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('CA', 'en-GB')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('CA', 'en-US')),
  assert.equals('NEW_TAB_DE_DE', newtab.scheduled_surface_id_v1('DE', 'de')),
  assert.equals('NEW_TAB_DE_DE', newtab.scheduled_surface_id_v1('DE', 'de-AT')),
  assert.equals('NEW_TAB_DE_DE', newtab.scheduled_surface_id_v1('DE', 'de-CH')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('GB', 'en-CA')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('GB', 'en-GB')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('GB', 'en-US')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('IE', 'en-CA')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('IE', 'en-GB')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('IE', 'en-US')),
  assert.equals('NEW_TAB_FR_FR', newtab.scheduled_surface_id_v1('FR', 'fr')),
  assert.equals('NEW_TAB_IT_IT', newtab.scheduled_surface_id_v1('IT', 'it')),
  assert.equals('NEW_TAB_ES_ES', newtab.scheduled_surface_id_v1('ES', 'es')),
  assert.equals('NEW_TAB_EN_INTL', newtab.scheduled_surface_id_v1('IN', 'en-CA')),
  assert.equals('NEW_TAB_EN_INTL', newtab.scheduled_surface_id_v1('IN', 'en-GB')),
  assert.equals('NEW_TAB_EN_INTL', newtab.scheduled_surface_id_v1('IN', 'en-US')),
  assert.equals('NEW_TAB_DE_DE', newtab.scheduled_surface_id_v1('CH', 'de')),
  assert.equals('NEW_TAB_DE_DE', newtab.scheduled_surface_id_v1('AT', 'de')),
  assert.equals('NEW_TAB_DE_DE', newtab.scheduled_surface_id_v1('BE', 'de')),
  -- # Locale can be a main language only.
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('CA', 'en')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('US', 'en')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('GB', 'en')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('IE', 'en')),
  assert.equals('NEW_TAB_EN_INTL', newtab.scheduled_surface_id_v1('IN', 'en')),
  -- # The locale language primarily determines the market, even if it's not the most common language in the region.
  assert.equals('NEW_TAB_DE_DE', newtab.scheduled_surface_id_v1('US', 'de')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('FR', 'en')),
  assert.equals('NEW_TAB_ES_ES', newtab.scheduled_surface_id_v1('DE', 'es')),
  assert.equals('NEW_TAB_FR_FR', newtab.scheduled_surface_id_v1('ES', 'fr')),
  assert.equals('NEW_TAB_IT_IT', newtab.scheduled_surface_id_v1('CA', 'it')),
  -- # Extract region from locale, if it is not explicitly provided.
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1(NULL, 'en-US')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1(NULL, 'en-GB')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1(NULL, 'en-IE')),
  -- # locale can vary in case.
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1(NULL, 'eN-US')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1(NULL, 'En-GB')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1(NULL, 'EN-ie')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1(NULL, 'en-cA')),
  -- # region can vary in case.
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('gB', 'en')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('Ie', 'en')),
  assert.equals('NEW_TAB_EN_INTL', newtab.scheduled_surface_id_v1('in', 'en')),
  -- # Default to international NewTab when region is unknown.
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('XX', 'en')),
  -- # Default to English when language is unknown.
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('US', 'xx')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('CA', 'xx')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('GB', 'xx')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('IE', 'xx')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('YY', 'xx'));
