CREATE OR REPLACE FUNCTION serp_events.is_ad_component(component STRING)
RETURNS BOOL AS (
  COALESCE(
    component IN (
      'ad_carousel',
      'ad_image_row',
      'ad_link',
      'ad_sidebar',
      'ad_sitelink',
      'ad_uncategorized'
    ),
    FALSE
  )
);

-- Tests
SELECT
  assert.equals(TRUE, serp_events.is_ad_component('ad_link')),
  assert.equals(FALSE, serp_events.is_ad_component('other'));
