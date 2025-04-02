
                -- The value returned by newtab.scheduled_surface_id_v1 represents the Content teams reference for the
                -- surface where an article is published on the Newtab.
                -- This UDF is composed based on the merino backend implementation here:
                -- https://github.com/mozilla-services/merino-py/blob/main/merino/curated_recommendations/provider.py#L66-L103
CREATE OR REPLACE FUNCTION newtab.scheduled_surface_id_v1(country STRING, locale STRING)
RETURNS STRING AS (
  CASE
    WHEN SPLIT(locale, '-')[0] = 'de'
      THEN 'NEW_TAB_DE_DE'
    WHEN SPLIT(locale, '-')[0] = 'es'
      THEN 'NEW_TAB_ES_ES'
    WHEN SPLIT(locale, '-')[0] = 'fr'
      THEN 'NEW_TAB_FR_FR'
    WHEN SPLIT(locale, '-')[0] = 'it'
      THEN 'NEW_TAB_IT_IT'
    WHEN SPLIT(locale, '-')[0] = 'en'
      AND (SPLIT(locale, '-')[1] IN ('GB', 'IE') OR country IN ('GB', 'IE'))
      THEN 'NEW_TAB_EN_GB'
    WHEN SPLIT(locale, '-')[0] = 'en'
      AND (SPLIT(locale, '-')[1] = 'IN' OR country = 'IN')
      THEN 'NEW_TAB_EN_INTL'
    WHEN SPLIT(locale, '-')[0] = 'en'
      AND (SPLIT(locale, '-')[1] IN ('US', 'CA') OR country IN ('US', 'CA'))
      THEN 'NEW_TAB_EN_US'
    ELSE 'NEW_TAB_EN_US'
  END
);

                -- Tests
SELECT
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('US', 'en-US')),
  assert.equals('NEW_TAB_EN_GB', newtab.scheduled_surface_id_v1('IE', 'en-IE')),
  assert.equals('NEW_TAB_EN_INTL', newtab.scheduled_surface_id_v1('IN', 'en-US')),
  assert.equals('NEW_TAB_EN_US', newtab.scheduled_surface_id_v1('CA', 'en-US'));
