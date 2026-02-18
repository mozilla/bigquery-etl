CREATE OR REPLACE FUNCTION norm.subplat_attribution_channel_group(utm_source STRING)
RETURNS STRING AS (
  CASE
    -- Marketing Owned
    WHEN utm_source LIKE ANY(
        '%mozilla.org-whatsnew%',
        '%mozilla.org-welcome%',
        '%blog.mozilla.org%',
        '%fxnews%',
        '%fxavpn%',
        '%fxatips%',
        '%fxakip%',
        '%vpnwaitlist%',
        '%twitter.com%',
        '%fxaonboardingemail%',
        '%invite%',
        '%pockethits%',
        '%pkt-hits%',
        '%sync-onboarding%',
        '%stage.fxprivaterelay.nonprod.cloudops.mozgcp.net%',
        '%instagram%',
        '%wrapped_email%',
        '%relay-onboarding%',
        '%oim%',
        '%firefox-desktop%'
      )
      THEN 'Marketing Owned'
    -- Direct
    WHEN utm_source IN (
        'www.mozilla.org-vpn-product-page',
        'google-play',
        'product',
        'www.mozilla.org-vpn-info'
      )
      THEN 'Direct'
    -- Product Owned
    WHEN utm_source LIKE ANY(
        '%about-prefs%',
        '%about-preferences%',
        '%leanplum-push-notification%',
        '%firefox-browser%',
        '%newtab%',
        '%monitor.firefox.com%',
        '%fx-monitor%',
        '%relay-firefox-com.translate.goog%',
        '%accounts.firefox.com%',
        '%privatebrowser%',
        '%pocket%',
        '%fx-vpn-windows%',
        '%toolbar%',
        '%spotlight-modal%',
        '%thunderbird%',
        '%sponsoredtile%',
        '%activity-stream%',
        '%mozilla.org-firefox-accounts%',
        '%mozilla.org-firefox_home%',
        '%send.firefox.com%',
        '%addons.mozilla.org%',
        '%premium.firefox.com%',
        '%fpn.firefox.com%',
        '%leanplum-push-qa%',
        '%fx-ios-vpn%',
        '%modal%',
        '%fx-relay-addon%',
        '%fx-relay%',
        '%mozilla.org-firefox-browsers%',
        '%new-tab-ad%',
        '%pocket_saves%',
        '%www.mozilla.org-vpn-or-proxy%',
        '%fx-vpn-iOSs%'
      )
      THEN 'Product Owned'
    -- Marketing Paid
    WHEN utm_source LIKE ANY(
        '%facebook%',
        '%instagram%',
        '%google%',
        '%saasworthy.com%',
        '%youtube%'
      )
      OR utm_source IN ('reddit', 'dv360')
      THEN 'Marketing Paid'
    -- Miscellaneous (catch-all for remaining patterns)
    WHEN utm_source LIKE ANY('%relay%', '%desktop-signup-flow%', '%multi.account.containers%')
      OR utm_source IN (
        'invalid',
        'yahoo',
        'bdmtools',
        'private-relay',
        'Blog',
        'duckduckgo',
        'bing',
        'FuckOff',
        '(not set)',
        'baidu',
        'chrome',
        'vpnsite',
        'gk_test',
        'yandex',
        'manual_testing',
        'demo_1_server',
        'Drippler',
        'vpn.',
        'saashub',
        'addon',
        'teaching-the-peeps',
        'test0706',
        'pocket_mylist',
        'devtools',
        'fpn-default',
        'fxa'
      )
      THEN 'Miscellaneous'
    -- Default for any unmapped values (based on the data, this should be Marketing Owned for most mozilla domains)
    WHEN utm_source LIKE '%mozilla.org%'
      THEN 'Marketing Owned'
    -- Final fallback
    ELSE 'Miscellaneous'
  END
);
