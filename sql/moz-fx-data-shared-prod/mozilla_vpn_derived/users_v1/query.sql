WITH raw AS (
  SELECT
    id,
    fxa_uid,
    created_at,
    mozfun.json.js_extract_string_map(attribution) AS attribution,
  FROM
    mozilla_vpn_external.users_v1
),
intermediate AS (
  SELECT
    *,
    CASE
      -- First, extract the nicely labelled attribution.
      -- From whichever random field the nice label was crammed into.
    WHEN
      mozfun.map.get_key(attribution, 'utm_campaign') IN ('private-browsing-vpn-link')
    THEN
      mozfun.map.get_key(attribution, 'utm_campaign')
    WHEN
      mozfun.map.get_key(attribution, 'referrer') IN (
        'app-store-ios',
        'app-store-windows',
        'app-store-android'
      )
    THEN
      mozfun.map.get_key(attribution, 'referrer')
    WHEN
      mozfun.map.get_key(attribution, 'utm_medium') IN (
        'snippet',
        'email',
        'secure-proxy',
        'devtools_whatsnew'
      )
    THEN
      mozfun.map.get_key(attribution, 'utm_medium')
    WHEN
      mozfun.map.get_key(attribution, 'utm_source') IN ('leanplum-push-notification')
    THEN
      mozfun.map.get_key(attribution, 'utm_source')
      -- Clean up some unambiguous mess
    WHEN
      mozfun.map.get_key(attribution, 'referrer') = 'app-store-macos'
    THEN
      'app-store-ios'
    WHEN
      mozfun.map.get_key(attribution, 'utm_source') = 'google-play'
    THEN
      'app-store-android'
    WHEN
      mozfun.map.get_key(attribution, 'referrer') = 'android-app://org.mozilla.firefox.vpn/'
    THEN
      'android vpn app'
    WHEN
      mozfun.map.get_key(attribution, 'utm_source') = 'monitor.firefox.com'
    THEN
      'monitor'
    WHEN
      attribution IS NULL
    THEN
      'no attribution'
      -- Deal with the somewhat ambiguous, potentially-overlapping categories
    WHEN
      mozfun.map.get_key(attribution, 'referrer') = 'https://monitor.firefox.com/'
        -- Unclear whether this is a real web referral, or an in-product referral?
        -- Especially since the tracking codes are missing.
    THEN
      'monitor'
    WHEN
      STARTS_WITH(mozfun.map.get_key(attribution, 'utm_source'), 'mozilla.org-whatsnew')
    THEN
      'whatsnew page'
    WHEN
      mozfun.map.get_key(attribution, 'utm_source') = 'firefox-browser'
      AND STARTS_WITH(mozfun.map.get_key(attribution, 'utm_campaign'), 'about-protections')
    THEN
      'about:protections'
    WHEN
      mozfun.map.get_key(attribution, 'utm_campaign') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_medium') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_source') IS NULL
      AND mozfun.map.get_key(attribution, 'referrer') IN (
        'https://www.google.com/',
        'https://duckduckgo.com/',
        'https://html.duckduckgo.com/',
        'https://www.bing.com/',
          -- Should add more domains as necessary - or find an authoritative whitelist
        'https://www.google.co.uk/'
      )
    THEN
      'organic search'
    WHEN
      attribution IS NOT NULL
      AND mozfun.map.get_key(attribution, 'utm_campaign') IS NULL
      AND mozfun.map.get_key(attribution, 'referrer') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_medium') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_source') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_content') IS NULL
    THEN
      'blank but not null'
    WHEN
      mozfun.map.get_key(attribution, 'utm_campaign') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_medium') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_source') IS NULL
      AND mozfun.map.get_key(attribution, 'utm_content') IS NULL
      AND mozfun.map.get_key(attribution, 'referrer') IS NOT NULL
    THEN
      'untracked link'
      -- Deal with the dumpster fires
    WHEN
      ENDS_WITH(mozfun.map.get_key(attribution, 'referrer'), 'mozilla.org/')
        -- N.B. this needs lower precedence than 'whatsnew page' and possibly
        -- other in-product channels, which have 'mozilla.org' as a referrer. Ergh.
        -- N.B. I gave this lower precedence than "untracked link",
        -- which means this only includes referrals with UTM codes present.
        -- I assumed that if UTM codes are absent then it's not an intentional
        -- referral??
    THEN
      'mozilla website referral'
    WHEN
      mozfun.map.get_key(attribution, 'utm_source') = 'blog.mozilla.org'
        -- Lots of referrals from v1.0 (and some from 1.1) have a utm_source
        -- from blog.mozilla.org, but either no referrer or a non-mozilla referrer
        -- (e.g. theverge, reddit, etc).
    THEN
      'blog.m.o organic'
    ELSE
      'other'
    END
    AS attribution_category,
  FROM
    raw
)
SELECT
  *,
  CASE
  WHEN
    attribution_category IN ('app-store-android', 'app-store-ios', 'android vpn app')
  THEN
    'mobile apps/stores'
  WHEN
    attribution_category IN ('untracked link', 'blog.m.o organic')
  THEN
    'non-mozilla web referral'
  WHEN
    attribution_category IN (
      'devtools_whatsnew',
      'about:protections',
      'private-browsing-vpn-link',
      'snippet'
        -- 'whatsnew page' -- big enough to be its own category
    )
  THEN
    'other in-browser'
  WHEN
    attribution_category IN ('other', 'app-store-windows', 'secure-proxy')
  THEN
    'other'
  WHEN
    attribution_category IN ('no attribution', 'blank but not empty?!')
  THEN
    'no attribution'
  ELSE
    attribution_category
  END
  AS coarse_attribution_category
FROM
  intermediate
