-- The value returned by newtab.surface_id_country represents country parsed from the surface_id.
CREATE OR REPLACE FUNCTION newtab.surface_id_country(
  surface_id STRING,
  locale STRING,
  country STRING
)
RETURNS STRING AS (
  CASE
    WHEN surface_id IS NULL
      AND locale IS NULL
      AND country IS NULL
      THEN NULL
    WHEN surface_id = 'NEW_TAB_EN_INTL'
      THEN 'INTL'
    WHEN surface_id LIKE 'NEW_TAB_%'
      THEN
        CASE
          WHEN country IS NOT NULL
            THEN country
          ELSE RIGHT(surface_id, 2)
        END
    WHEN surface_id IS NULL
      OR surface_id = ''
      THEN UPPER(RIGHT(LOWER(locale), 2))
    ELSE country
  END
);

-- Tests
SELECT
  assert.equals('INTL', newtab.surface_id_country('NEW_TAB_EN_INTL', LOWER('en-US'), NULL)),
  assert.equals('FR', newtab.surface_id_country('NEW_TAB_FR_FR', LOWER('fr'), NULL)),
  assert.equals('GB', newtab.surface_id_country('NEW_TAB_EN_GB', LOWER('en-US'), NULL)),
  assert.equals('US', newtab.surface_id_country('NEW_TAB_EN_US', LOWER('en-US'), NULL)),
  assert.equals('DE', newtab.surface_id_country('NEW_TAB_DE_DE', LOWER('de'), 'DE')),
  assert.equals('DE', newtab.surface_id_country('NEW_TAB_DE_DE', LOWER('de'), NULL)),
  assert.equals('AT', newtab.surface_id_country('NEW_TAB_DE_DE', LOWER('de'), 'AT')),
  assert.equals('CA', newtab.surface_id_country(NULL, LOWER('en-CA'), NULL)),
  assert.equals('CN', newtab.surface_id_country(NULL, LOWER('zh-CN'), NULL)),
  assert.equals('FR', newtab.surface_id_country(NULL, LOWER('fr'), NULL)),
  assert.equals('GB', newtab.surface_id_country(NULL, LOWER('en-GB'), NULL)),
  assert.equals('ID', newtab.surface_id_country(NULL, LOWER('id'), NULL)),
  assert.equals('US', newtab.surface_id_country(NULL, LOWER('en-US'), NULL)),
  assert.equals('CA', newtab.surface_id_country('', LOWER('en-CA'), NULL)),
  assert.equals('CN', newtab.surface_id_country('', LOWER('zh-CN'), NULL)),
  assert.equals('DE', newtab.surface_id_country('', LOWER('de'), NULL)),
  assert.equals('ES', newtab.surface_id_country('', LOWER('es-ES'), NULL)),
  assert.equals('FR', newtab.surface_id_country('', LOWER('fr'), NULL)),
  assert.equals('GB', newtab.surface_id_country('', LOWER('en-GB'), NULL)),
  assert.equals('ID', newtab.surface_id_country('', LOWER('id'), NULL)),
  assert.equals('PL', newtab.surface_id_country('', LOWER('pl'), NULL)),
  assert.equals('RU', newtab.surface_id_country('', LOWER('ru'), NULL)),
  assert.equals('TW', newtab.surface_id_country('', LOWER('zh-TW'), NULL)),
  assert.equals('US', newtab.surface_id_country('', LOWER('en-US'), NULL));
