-- Maps raw product identifiers from CX data sources (GA4 page paths, Zendesk
-- ticket fields, Kitsune question fields) to the canonical product name used
-- in CX reporting. Replaces the previous static lookup table
-- `moz-fx-data-shared-prod.static.cx_product_mappings_v1`.
--
-- Behavior:
--   - Exact matches preserve every mapping that existed in the static table.
--   - GA4 falls through to substring rules so newly-coined paths (e.g.
--     /firefox/firefox-enterprise/mobile/) map automatically.
--   - Unknown inputs return 'Other' instead of leaking the raw value, so
--     downstream consumers no longer need a COALESCE fallback. Pair with a
--     checks.sql warning to surface novel raw values for review.
--   - NULL input returns NULL.
CREATE OR REPLACE FUNCTION customer_experience.normalize_product(raw STRING, source STRING)
RETURNS STRING AS (
  CASE
    WHEN raw IS NULL
      THEN NULL
    -- ----- GA4: exact-match paths (preserves data.csv behavior) -----
    WHEN source = 'GA4'
      AND raw IN (
        '/firefox-preview/',
        '/privacy-and-security/firefox/',
        '/firefox/',
        '/firefox-preview/mobile/',
        '/firefox/focus-firefox/ios/mobile/',
        '/firefox/ios/mobile/',
        '/firefox/privacy-and-security/'
      )
      THEN 'Firefox'
    WHEN source = 'GA4'
      AND raw IN (
        '/firefox-android-esr/mobile/',
        '/mobile/',
        '/firefox/mobile/',
        '/firefox-android-esr/',
        '/ios/mobile/'
      )
      THEN 'Firefox Android'
    WHEN source = 'GA4'
      AND raw IN (
        '/firefox/firefox-enterprise/',
        '/firefox/mobile/firefox-enterprise/',
        '/firefox-preview/firefox/mobile/firefox-enterprise/',
        '/firefox-enterprise/'
      )
      THEN 'Firefox Enterprise'
    WHEN source = 'GA4'
      AND raw IN (
        '/focus-firefox/',
        '/klar/',
        '/firefox/mobile/ios/focus-firefox/',
        '/firefox-preview/mobile/ios/focus-firefox/',
        '/firefox/mobile/focus-firefox/',
        '/mobile/ios/focus-firefox/',
        '/mobile/focus-firefox/',
        '/focus-firefox/klar/'
      )
      THEN 'Firefox Focus'
    WHEN source = 'GA4'
      AND raw IN ('/ios/', '/mobile/ios/', '/firefox/mobile/ios/')
      THEN 'Firefox iOS'
    WHEN source = 'GA4'
      AND raw IN ('/mobile/relay/', '/relay/', '/firefox/relay/')
      THEN 'Firefox Relay'
    WHEN source = 'GA4'
      AND raw = '/mdn-plus/'
      THEN 'Mdn Plus'
    WHEN source = 'GA4'
      AND raw IN (
        '/mozilla-account/',
        '/mozilla-account/ios/',
        '/mozilla-account/hubs/',
        '/mozilla-account/mozilla-vpn/',
        '/firefox/mozilla-account/mobile/ios/',
        '/privacy-and-security/mozilla-account/',
        '/firefox/mozilla-account/mobile/',
        '/firefox/pocket/mozilla-account/',
        '/mozilla-account/mozilla-vpn/relay/',
        '/mozilla-account/mobile/',
        '/firefox/mozilla-account/',
        '/mozilla-account/mdn-plus/',
        '/mozilla-account/relay/',
        '/firefox-lockwise/mobile/mozilla-account/',
        '/firefox/ios/mobile/mozilla-account/',
        '/ios/mozilla-account/',
        '/mozilla-account/privacy-and-security/'
      )
      THEN 'Mozilla Account'
    WHEN source = 'GA4'
      AND raw IN ('/firefox/monitor/', '/monitor/')
      THEN 'Mozilla Monitor'
    WHEN source = 'GA4'
      AND raw IN (
        '/mozilla-vpn/',
        '/firefox/mozilla-vpn/',
        '/mozilla-vpn/relay/',
        '/firefox-private-network/',
        '/firefox-private-network-vpn/',
        '/firefox/firefox-private-network-vpn/'
      )
      THEN 'Mozilla VPN'
    WHEN source = 'GA4'
      AND raw IN (
        '/firefox/mobile/thunderbird/',
        '/thunderbird/',
        '/firefox/mobile/ios/thunderbird/',
        '/firefox/thunderbird/'
      )
      THEN 'Thunderbird'
    WHEN source = 'GA4'
      AND raw IN (
        '/thunderbird-android/',
        '/thunderbird/thunderbird-android/',
        '/firefox/mobile/ios/thunderbird/thunderbird-android/'
      )
      THEN 'Thunderbird Android'
    WHEN source = 'GA4'
      AND raw IN (
        '/privacy-and-security/',
        '/firefox/pocket/',
        '/contributor/',
        '/open-badges/',
        '/firefox-os/',
        '/firefox-lite/',
        '/firefox/firefox-os/mobile/ios/',
        '/firefox-fire-tv/',
        '/firefox-fire-tv/firefox-amazon-devices/',
        '/firefox/webmaker/ios/',
        '/privacy-and-security/firefox-lite/firefox-reality/firefox/mobile/',
        '/pocket/',
        '/hubs/',
        '/firefox-lockwise/firefox/',
        '/contributor/firefox-os/',
        '/contributor/mobile/',
        '/contributor/firefox/firefox-os/mobile/',
        '/firefox/pocket/mobile/ios/',
        '/firefox-lockwise/',
        '/firefox/firefox-os/mobile/',
        '/firefox-reality/',
        '/contributor/firefox/',
        '/contributor/firefox/webmaker/firefox-os/mobile/',
        '/screenshot-go/',
        '/firefox-lite/mobile/',
        '/firefox-send/',
        '/contributor/thunderbird/',
        '/webmaker/',
        '/contributor/firefox/webmaker/firefox-os/firefox-windows-8-touch/open-badges/mobile/thunderbird/',
        '/firefox-windows-8-touch/',
        '/firefox-amazon-devices/',
        '/firefox-send/firefox/',
        '/firefox/mobile/ios/focus-firefox/thunderbird/thunderbird-android/firefox-enterprise/',
        '/firefox/mobile/ios/focus-firefox/thunderbird/thunderbird-android/',
        '/firefox-preview/firefox/hubs/mobile/ios/thunderbird/firefox-enterprise/',
        '/firefox/mobile/ios/thunderbird/thunderbird-android/firefox-enterprise/'
      )
      THEN 'Other'
    -- ----- GA4: substring fallback for novel paths -----
    -- Ordering matters: most-specific brand markers fire before broader ones.
    -- "Contributor" paths are bucketed as Other before product detection so
    -- that e.g. /contributor/thunderbird/ does not map to Thunderbird.
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/contributor/')
      THEN 'Other'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(
        LOWER(raw),
        r'/(firefox-os|firefox-lite|firefox-reality|firefox-lockwise|firefox-fire-tv|firefox-amazon-devices|firefox-send|firefox-windows-8-touch|firefox-preview|webmaker|pocket|hubs|screenshot-go|open-badges)/'
      )
      THEN 'Other'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/mozilla-account/')
      THEN 'Mozilla Account'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(
        LOWER(raw),
        r'/(mozilla-vpn|firefox-private-network|firefox-private-network-vpn)/'
      )
      THEN 'Mozilla VPN'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/relay/')
      THEN 'Firefox Relay'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/monitor/')
      THEN 'Mozilla Monitor'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/mdn-plus/')
      THEN 'Mdn Plus'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/(focus-firefox|klar)/')
      THEN 'Firefox Focus'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/thunderbird-android/')
      THEN 'Thunderbird Android'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/thunderbird/')
      THEN 'Thunderbird'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/firefox-enterprise/')
      THEN 'Firefox Enterprise'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/(firefox-android-esr|mobile|ios)/')
      AND NOT REGEXP_CONTAINS(LOWER(raw), r'/firefox-ios/')
      THEN 'Firefox Android'
    WHEN source = 'GA4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/firefox/')
      THEN 'Firefox'
    WHEN source = 'GA4'
      THEN 'Other'
    -- ----- Zendesk -----
    WHEN source = 'Zendesk'
      AND raw IN ('mozilla-account', 'firefox_accounts')
      THEN 'Mozilla Account'
    WHEN source = 'Zendesk'
      AND raw IN ('relay', 'product_relay', 'privacy_protection_bundle')
      THEN 'Firefox Relay'
    WHEN source = 'Zendesk'
      AND raw = 'monitor'
      THEN 'Mozilla Monitor'
    WHEN source = 'Zendesk'
      AND raw = 'mdn-plus'
      THEN 'Mdn Plus'
    WHEN source = 'Zendesk'
      AND raw IN ('firefox-android', 'firefox-android-reviews')
      THEN 'Firefox Android'
    WHEN source = 'Zendesk'
      AND raw IN ('firefox-ios', 'firefox-ios-reviews')
      THEN 'Firefox iOS'
    WHEN source = 'Zendesk'
      AND raw IN (
        'firefox-private-network-vpn',
        'firefox-private-network',
        'mozilla-vpn',
        'vpn_relay_bundle'
      )
      THEN 'Mozilla VPN'
    WHEN source = 'Zendesk'
      AND raw IN ('X', 'product_other', 'pocket', 'hubs')
      THEN 'Other'
    WHEN source = 'Zendesk'
      THEN 'Other'
    -- ----- Kitsune -----
    WHEN source = 'Kitsune'
      AND raw = 'firefox'
      THEN 'Firefox'
    WHEN source = 'Kitsune'
      AND raw = 'mobile'
      THEN 'Firefox Android'
    WHEN source = 'Kitsune'
      AND raw = 'ios'
      THEN 'Firefox iOS'
    WHEN source = 'Kitsune'
      AND raw = 'focus-firefox'
      THEN 'Firefox Focus'
    WHEN source = 'Kitsune'
      AND raw = 'firefox-enterprise'
      THEN 'Firefox Enterprise'
    WHEN source = 'Kitsune'
      AND raw = 'thunderbird'
      THEN 'Thunderbird'
    WHEN source = 'Kitsune'
      AND raw = 'thunderbird-android'
      THEN 'Thunderbird Android'
    WHEN source = 'Kitsune'
      AND raw = 'monitor'
      THEN 'Mozilla Monitor'
    WHEN source = 'Kitsune'
      AND raw = 'relay'
      THEN 'Firefox Relay'
    WHEN source = 'Kitsune'
      AND raw IN ('mozilla-vpn', 'firefox-private-network')
      THEN 'Mozilla VPN'
    WHEN source = 'Kitsune'
      AND raw = 'firefox-preview'
      THEN 'Firefox'
    WHEN source = 'Kitsune'
      AND raw IN (
        'firefox-amazon-devices',
        'firefox-fire-tv',
        'firefox-lite',
        'firefox-lockwise',
        'firefox-os',
        'firefox-reality',
        'firefox-windows-8-touch',
        'hubs',
        'webmaker'
      )
      THEN 'Other'
    WHEN source = 'Kitsune'
      THEN 'Other'
    ELSE 'Other'
  END
);

-- Tests
SELECT
  assert.null(customer_experience.normalize_product(NULL, 'GA4')),
  -- GA4: exact-match, substring fallback, precedence (Other-before-product,
  -- more-specific-brand-before-Android), and catch-all.
  assert.equals('Firefox', customer_experience.normalize_product('/firefox/', 'GA4')),
  assert.equals(
    'Firefox Enterprise',
    customer_experience.normalize_product('/firefox/firefox-enterprise/mobile/', 'GA4')
  ),
  assert.equals('Other', customer_experience.normalize_product('/contributor/thunderbird/', 'GA4')),
  assert.equals(
    'Mozilla Account',
    customer_experience.normalize_product('/mobile/mozilla-account/', 'GA4')
  ),
  assert.equals('Other', customer_experience.normalize_product('/something-new/', 'GA4')),
  -- Zendesk: known mapping and unknown fallback.
  assert.equals(
    'Mozilla Account',
    customer_experience.normalize_product('mozilla-account', 'Zendesk')
  ),
  assert.equals('Other', customer_experience.normalize_product('something-new', 'Zendesk')),
  -- Kitsune: known mapping and unknown fallback.
  assert.equals('Firefox', customer_experience.normalize_product('firefox', 'Kitsune')),
  assert.equals('Other', customer_experience.normalize_product('something-new', 'Kitsune'))
