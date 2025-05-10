
-- newtab.is_default_ui_v1 computes if the newtab opened action is attributed as default_ui
CREATE OR REPLACE FUNCTION newtab.is_default_ui_v1(
  event_category STRING,
  event_name STRING,
  event_details ARRAY<STRUCT<key STRING, value STRING>>,
  newtab_homepage_category STRING,
  newtab_newtab_category STRING
)
RETURNS BOOLEAN AS (
  (
    event_category = 'newtab'
    AND event_name = 'opened'
    AND (
      (
        mozfun.map.get_key(event_details, 'source') = 'about:home'
        AND newtab_homepage_category = 'enabled'
      )
      OR (
        mozfun.map.get_key(event_details, 'source') = 'about:newtab'
        AND newtab_newtab_category = 'enabled'
      )
    )
  )
);

-- Tests
SELECT
  assert.true(
    newtab.is_default_ui_v1(
      'newtab',
      'opened',
      [STRUCT('foo' AS key, 'bar' AS value), ('source', 'about:home')],
      'enabled',
      'extension'
    )
  ),
  assert.false(
    newtab.is_default_ui_v1(
      'newtab',
      'opened',
      [STRUCT('foo' AS key, 'bar' AS value), ('source', 'about:welcome')],
      'enabled',
      'enabled'
    )
  ),
  assert.true(
    newtab.is_default_ui_v1(
      'newtab',
      'opened',
      [STRUCT('foo' AS key, 'bar' AS value), ('source', 'about:newtab')],
      'other',
      'enabled'
    )
  );
