CREATE OR REPLACE FUNCTION norm.subplat_utm_to_channel_group(utm_source STRING)
RETURNS STRING AS (
  CASE
    -- Marketing Owned
    WHEN utm_source LIKE '%mozilla.org-whatsnew%'
      OR utm_source LIKE '%mozilla.org-welcome%'
      OR utm_source LIKE '%blog.mozilla.org%'
      OR utm_source LIKE '%fxnews%'
      OR utm_source LIKE '%fxavpn%'
      OR utm_source LIKE '%fxatips%'
      OR utm_source LIKE '%fxakip%'
      OR utm_source LIKE '%vpnwaitlist%'
      OR utm_source LIKE '%monitor%'
      OR utm_source LIKE '%twitter.com%'
      OR utm_source LIKE '%fxaonboardingemail%'
      OR utm_source LIKE '%invite%'
      OR utm_source LIKE '%pockethits%'
      OR utm_source LIKE '%pkt-hits%'
      OR utm_source LIKE '%news%'
      OR utm_source LIKE '%sync-onboarding%'
      OR utm_source LIKE '%stage.fxprivaterelay.nonprod.cloudops.mozgcp.net%'
      OR utm_source LIKE '%instagram%'
      OR utm_source LIKE '%wrapped_email%'
      OR utm_source LIKE '%relay-onboarding%'
      OR utm_source LIKE '%oim%'
      OR utm_source LIKE '%firefox-desktop%'
      THEN 'Marketing Owned'
    -- Direct
    WHEN utm_source = 'www.mozilla.org-vpn-product-page'
      OR utm_source = 'google-play'
      OR utm_source = 'product'
      THEN 'Direct'
    -- Product Owned
    WHEN utm_source LIKE '%about-prefs%'
      OR utm_source LIKE '%leanplum-push-notification%'
      OR utm_source LIKE '%firefox-browser%'
      OR utm_source LIKE '%newtab%'
      OR utm_source LIKE '%monitor.firefox.com%'
      OR utm_source LIKE '%fx-monitor%'
      OR utm_source LIKE '%relay-firefox-com.translate.goog%'
      OR utm_source LIKE '%accounts.firefox.com%'
      OR utm_source LIKE '%privatebrowser%'
      OR utm_source LIKE '%pocket%'
      OR utm_source LIKE '%fx-vpn-windows%'
      OR utm_source LIKE '%toolbar%'
      OR utm_source LIKE '%spotlight-modal%'
      OR utm_source LIKE '%thunderbird%'
      OR utm_source LIKE '%sponsoredtile%'
      OR utm_source LIKE '%activity-stream%'
      OR utm_source LIKE '%mozilla.org-firefox-accounts%'
      OR utm_source LIKE '%mozilla.org-firefox_home%'
      OR utm_source LIKE '%send.firefox.com%'
      OR utm_source LIKE '%addons.mozilla.org%'
      OR utm_source LIKE '%premium.firefox.com%'
      OR utm_source LIKE '%fpn.firefox.com%'
      OR utm_source LIKE '%leanplum-push-qa%'
      OR utm_source LIKE '%fx-ios-vpn%'
      OR utm_source LIKE '%modal%'
      OR utm_source LIKE '%fx-relay-addon%'
      OR utm_source LIKE '%fx-relay%'
      OR utm_source LIKE '%mozilla.org-firefox-browsers%'
      OR utm_source LIKE '%new-tab-ad%'
      OR utm_source LIKE '%firefox%'
      OR utm_source LIKE '%pocket_saves%'
      OR utm_source LIKE '%www.mozilla.org-vpn-or-proxy%'
      OR utm_source LIKE '%fx-vpn-iOSs%'
      THEN 'Product Owned'
    -- Marketing Paid
    WHEN utm_source LIKE '%facebook%'
      OR utm_source LIKE '%instagram%'
      OR utm_source LIKE '%google%'
      OR utm_source = 'reddit'
      OR utm_source = 'dv360'
      OR utm_source LIKE '%saasworthy.com%'
      OR utm_source LIKE '%youtube%'
      THEN 'Marketing Paid'
    -- Miscellaneous (catch-all for remaining patterns)
    WHEN utm_source LIKE '%relay%'
      OR utm_source LIKE '%desktop-signup-flow%'
      OR utm_source = 'invalid'
      OR utm_source LIKE '%multi.account.containers%'
      OR utm_source IN (
        'yahoo',
        'bdmtools',
        'private-relay',
        'Blog',
        'duckduckgo',
        'bing',
        'www.mozilla.org-vpn-info',
        'FuckOff',
        '(not set)',
        'baidu',
        'chrome',
        'vpnsite',
        'about-preferences',
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
      OR utm_source LIKE '%.mozilla.org%'
      THEN 'Marketing Owned'
    -- Final fallback
    ELSE 'Miscellaneous'
  END
);
