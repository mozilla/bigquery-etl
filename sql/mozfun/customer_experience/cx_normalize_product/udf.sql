--   Standardizes product names in the Customer Experience domain so they are
--   consistent across domains (Product, Marketing, etc.), implementing the
--   canonical product mapping defined by the Customer Experience team.
--   Maps a raw product identifier (GA4 page path, Zendesk custom_product or
--   Kitsune product slug) to the canonical CX product name.
--   Unknown inputs return 'unclassified' and NULL inputs return NULL, so
--   products we cannot recognize stay distinct from those we deliberately group
--   as 'Other'.
--
-- Canonical mapping (source of truth, maintained by the CX team): https://docs.google.com/spreadsheets/d/1NX_Kso5fm-NcfLQ6KxHGH3Jje0MSabdAkNnnyjvu1aI/edit?gid=2127849644#gid=2127849644
--
-- Behavior:
--   - Exact-match arms encode the per-source slugs and paths from the mapping above.
--   - GA4 falls through to substring rules so newly-coined paths (e.g.
--     /firefox/firefox-enterprise/mobile/) map automatically.
--   - Inputs that match a known-but-unsupported product (e.g. Pocket, Hubs,
--     contributor paths) return 'Other' so they are not leaked as raw values.
--   - Unknown/unmatched inputs return 'unclassified' so they are distinct from
--     both 'Other' (a deliberate bucket) and NULL (no value in the source).
--     Consumers should monitor this bucket to surface novel raw values that
--     need a classification rule added.
--   - NULL input returns NULL.
--   - Matching is case-insensitive: both `raw` and `source` are lowercased
--     before comparison, so e.g. '/Firefox/' or source 'ga4' still resolve.
CREATE OR REPLACE FUNCTION customer_experience.cx_normalize_product(raw STRING, source STRING)
RETURNS STRING AS (
  CASE
    WHEN raw IS NULL
      THEN NULL
    -- ----- GA4: exact-match paths (preserves data.csv behavior) -----
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '/firefox-preview/',
        '/privacy-and-security/firefox/',
        '/firefox/',
        '/firefox-preview/mobile/',
        '/firefox/privacy-and-security/'
      )
      THEN 'Firefox Desktop'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '/firefox-android-esr/mobile/',
        '/mobile/',
        '/firefox/mobile/',
        '/firefox-android-esr/'
      )
      THEN 'Fenix'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '/firefox/firefox-enterprise/',
        '/firefox/mobile/firefox-enterprise/',
        '/firefox-preview/firefox/mobile/firefox-enterprise/',
        '/firefox-enterprise/'
      )
      THEN 'Firefox Enterprise'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
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
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '/ios/',
        '/mobile/ios/',
        '/ios/mobile/',
        '/firefox/mobile/ios/',
        '/firefox/ios/mobile/'
      )
      THEN 'Firefox iOS'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN ('/mobile/relay/', '/relay/', '/firefox/relay/')
      THEN 'Firefox Relay'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) = '/mdn-plus/'
      THEN 'MDN Plus'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
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
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN ('/firefox/monitor/', '/monitor/')
      THEN 'Mozilla Monitor'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '/mozilla-vpn/',
        '/firefox/mozilla-vpn/',
        '/mozilla-vpn/relay/',
        '/firefox-private-network/',
        '/firefox-private-network-vpn/',
        '/firefox/firefox-private-network-vpn/'
      )
      THEN 'Mozilla VPN'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '/firefox/mobile/thunderbird/',
        '/thunderbird/',
        '/firefox/mobile/ios/thunderbird/',
        '/firefox/thunderbird/'
      )
      THEN 'Thunderbird'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '/thunderbird-android/',
        '/thunderbird/thunderbird-android/',
        '/firefox/mobile/ios/thunderbird/thunderbird-android/'
      )
      THEN 'Thunderbird Android'
    WHEN LOWER(source) = 'ga4'
      AND LOWER(raw) IN (
        '//',
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
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/contributor/')
      THEN 'Other'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(
        LOWER(raw),
        r'/(firefox-os|firefox-lite|firefox-reality|firefox-lockwise|firefox-fire-tv|firefox-amazon-devices|firefox-send|firefox-windows-8-touch|webmaker|pocket|hubs|screenshot-go|open-badges)/'
      )
      THEN 'Other'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/mozilla-account/')
      THEN 'Mozilla Account'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(
        LOWER(raw),
        r'/(mozilla-vpn|firefox-private-network|firefox-private-network-vpn)/'
      )
      THEN 'Mozilla VPN'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/relay/')
      THEN 'Firefox Relay'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/monitor/')
      THEN 'Mozilla Monitor'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/mdn-plus/')
      THEN 'MDN Plus'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/(focus-firefox|klar)/')
      THEN 'Firefox Focus'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/thunderbird-android/')
      THEN 'Thunderbird Android'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/thunderbird/')
      THEN 'Thunderbird'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/firefox-enterprise/')
      THEN 'Firefox Enterprise'
    -- 'mobile' is NOT an Android signal for GA4 — a /mobile/ path may well be
    -- iOS — so the mobile -> Fenix assumption applies only to Kitsune (handled
    -- by its exact 'mobile' rule below). For GA4 only an explicit Android marker
    -- maps to Fenix, and it is checked before iOS so it wins when a path carries
    -- both markers.
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/(firefox-android-esr|firefox-android)/')
      THEN 'Fenix'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/(firefox-ios|ios)/')
      THEN 'Firefox iOS'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/firefox-preview/')
      THEN 'Firefox Desktop'
    WHEN LOWER(source) = 'ga4'
      AND REGEXP_CONTAINS(LOWER(raw), r'/firefox/')
      THEN 'Firefox Desktop'
    WHEN LOWER(source) = 'ga4'
      THEN 'unclassified'
    -- ----- Zendesk -----
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) IN ('mozilla-account', 'firefox_accounts')
      THEN 'Mozilla Account'
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) IN ('relay', 'product_relay', 'privacy_protection_bundle')
      THEN 'Firefox Relay'
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) = 'monitor'
      THEN 'Mozilla Monitor'
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) = 'mdn-plus'
      THEN 'MDN Plus'
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) IN ('firefox-android', 'firefox-android-reviews')
      THEN 'Fenix'
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) IN ('firefox-ios', 'firefox-ios-reviews')
      THEN 'Firefox iOS'
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) IN (
        'firefox-private-network-vpn',
        'firefox-private-network',
        'mozilla-vpn',
        'vpn_relay_bundle'
      )
      THEN 'Mozilla VPN'
    WHEN LOWER(source) = 'zendesk'
      AND LOWER(raw) IN ('x', 'product_other', 'pocket', 'hubs')
      THEN 'Other'
    WHEN LOWER(source) = 'zendesk'
      THEN 'unclassified'
    -- ----- Kitsune -----
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'firefox'
      THEN 'Firefox Desktop'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'mobile'
      THEN 'Fenix'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'ios'
      THEN 'Firefox iOS'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'focus-firefox'
      THEN 'Firefox Focus'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'firefox-enterprise'
      THEN 'Firefox Enterprise'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'thunderbird'
      THEN 'Thunderbird'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'thunderbird-android'
      THEN 'Thunderbird Android'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'monitor'
      THEN 'Mozilla Monitor'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'relay'
      THEN 'Firefox Relay'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) IN ('mozilla-vpn', 'firefox-private-network')
      THEN 'Mozilla VPN'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) = 'firefox-preview'
      THEN 'Firefox Desktop'
    WHEN LOWER(source) = 'kitsune'
      AND LOWER(raw) IN (
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
    WHEN LOWER(source) = 'kitsune'
      THEN 'unclassified'
    ELSE 'unclassified'
  END
);

-- Tests
SELECT
  assert.null(customer_experience.cx_normalize_product(NULL, 'GA4')),
  -- GA4: exact-match, substring fallback, precedence (Other-before-product,
  -- more-specific-brand-before-Android, Android-before-iOS), and catch-all.
  assert.equals('Firefox Desktop', customer_experience.cx_normalize_product('/firefox/', 'GA4')),
  -- iOS substring fallback: generic /ios/ and the firefox-ios brand slug.
  assert.equals(
    'Firefox iOS',
    customer_experience.cx_normalize_product('/firefox/ios/help/', 'GA4')
  ),
  assert.equals(
    'Firefox iOS',
    customer_experience.cx_normalize_product('/firefox-ios/reviews/', 'GA4')
  ),
  -- GA4 'mobile' is not an Android signal in the substring fallback (mobile ->
  -- Fenix is Kitsune-only): a novel /firefox/mobile/... path resolves via the
  -- /firefox/ rule, and /mobile/ios/... resolves to iOS.
  assert.equals(
    'Firefox Desktop',
    customer_experience.cx_normalize_product('/firefox/mobile/whatsnew/', 'GA4')
  ),
  assert.equals(
    'Firefox iOS',
    customer_experience.cx_normalize_product('/mobile/ios/whatsnew/', 'GA4')
  ),
  -- Explicit GA4 Android marker maps to Fenix, and wins over iOS when both appear.
  assert.equals(
    'Fenix',
    customer_experience.cx_normalize_product('/firefox-android/whatsnew/', 'GA4')
  ),
  assert.equals(
    'Fenix',
    customer_experience.cx_normalize_product('/firefox-android/ios/whatsnew/', 'GA4')
  ),
  -- Legacy exact paths re-bucketed: ios+mobile now resolves to iOS, while the
  -- curated /mobile/ and /firefox/mobile/ exact paths stay Fenix.
  assert.equals('Firefox iOS', customer_experience.cx_normalize_product('/ios/mobile/', 'GA4')),
  assert.equals(
    'Firefox iOS',
    customer_experience.cx_normalize_product('/firefox/ios/mobile/', 'GA4')
  ),
  assert.equals('Fenix', customer_experience.cx_normalize_product('/mobile/', 'GA4')),
  assert.equals('Fenix', customer_experience.cx_normalize_product('/firefox/mobile/', 'GA4')),
  assert.equals(
    'Firefox Enterprise',
    customer_experience.cx_normalize_product('/firefox/firefox-enterprise/mobile/', 'GA4')
  ),
  assert.equals(
    'Other',
    customer_experience.cx_normalize_product('/contributor/thunderbird/', 'GA4')
  ),
  assert.equals(
    'Mozilla Account',
    customer_experience.cx_normalize_product('/mobile/mozilla-account/', 'GA4')
  ),
  assert.equals('unclassified', customer_experience.cx_normalize_product('/something-new/', 'GA4')),
  -- Known-but-unsupported products still bucket to 'Other' (not 'unclassified').
  assert.equals('Other', customer_experience.cx_normalize_product('/pocket/', 'GA4')),
  -- firefox-preview is Firefox Desktop (not Other), for exact and novel paths.
  assert.equals(
    'Firefox Desktop',
    customer_experience.cx_normalize_product('/firefox-preview/extensions/', 'GA4')
  ),
  -- Empty/malformed path '//' is bucketed as Other.
  assert.equals('Other', customer_experience.cx_normalize_product('//', 'GA4')),
  -- Firefox Focus mapping (klar / focus-firefox are the Focus brand markers).
  assert.equals('Firefox Focus', customer_experience.cx_normalize_product('/klar/', 'GA4')),
  -- Zendesk: known mapping and unknown fallback.
  assert.equals(
    'Mozilla Account',
    customer_experience.cx_normalize_product('mozilla-account', 'Zendesk')
  ),
  assert.equals(
    'unclassified',
    customer_experience.cx_normalize_product('something-new', 'Zendesk')
  ),
  -- Kitsune: known mapping and unknown fallback.
  assert.equals('Firefox Desktop', customer_experience.cx_normalize_product('firefox', 'Kitsune')),
  assert.equals(
    'unclassified',
    customer_experience.cx_normalize_product('something-new', 'Kitsune')
  ),
  -- Unrecognized source falls through to the final ELSE.
  assert.equals(
    'unclassified',
    customer_experience.cx_normalize_product('/firefox/', 'Salesforce')
  ),
  -- Matching is case-insensitive on both raw and source.
  assert.equals('Firefox Desktop', customer_experience.cx_normalize_product('/FIREFOX/', 'GA4')),
  assert.equals(
    'Mozilla Account',
    customer_experience.cx_normalize_product('Mozilla-Account', 'ZENDESK')
  )
