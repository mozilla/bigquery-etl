CREATE OR REPLACE FUNCTION addons.is_adblocker(addon_id STRING)
RETURNS BOOLEAN AS (
  addon_id IS NOT NULL
  AND addon_id IN (
    'uBlock0@raymondhill.net', -- 'uBlock_Origin'
    '{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}', -- 'AdBlock_Plus'
    'jid1-NIfFY2CA8fy1tg@jetpack', -- 'Adblock'
    'firefox@ghostery.com', -- 'Ghostery'
    'adblockultimate@adblockultimate.net' -- 'AdBlock_Ultimate'
  )
);

-- Tests
SELECT
  assert.true(addons.is_adblocker('uBlock0@raymondhill.net')),
  assert.true(addons.is_adblocker('adblockultimate@adblockultimate.net')),
  assert.false(addons.is_adblocker('uBlock_Origin')),
  assert.false(addons.is_adblocker('')),
  assert.false(addons.is_adblocker(NULL)),
