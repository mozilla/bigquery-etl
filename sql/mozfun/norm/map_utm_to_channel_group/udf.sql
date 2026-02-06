CREATE OR REPLACE FUNCTION norm.map_utm_to_channel_group(utm_source STRING)
RETURNS STRING AS (
  CASE
    -- Marketing Owned
    WHEN utm_source LIKE '%mozilla.org-whatsnew%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%mozilla.org-welcome%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%blog.mozilla.org%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%fxnews%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%fxavpn%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%fxatips%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%fxakip%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%vpnwaitlist%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%monitor%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%twitter.com%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%fxaonboardingemail%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%invite%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%pockethits%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%pkt-hits%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%news%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%sync-onboarding%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%stage.fxprivaterelay.nonprod.cloudops.mozgcp.net%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%instagram%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%wrapped_email%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%relay-onboarding%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%oim%'
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%firefox-desktop%'
      THEN 'Marketing Owned'
    -- Direct
    WHEN utm_source = 'www.mozilla.org-vpn-product-page'
      THEN 'Direct'
    WHEN utm_source = 'google-play'
      THEN 'Direct'
    WHEN utm_source = 'product'
      THEN 'Direct'
    -- Product Owned
    WHEN utm_source LIKE '%about-prefs%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%leanplum-push-notification%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%firefox-browser%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%newtab%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%monitor.firefox.com%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%fx-monitor%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%relay-firefox-com.translate.goog%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%accounts.firefox.com%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%privatebrowser%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%pocket%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%fx-vpn-windows%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%toolbar%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%spotlight-modal%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%thunderbird%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%sponsoredtile%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%activity-stream%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%mozilla.org-firefox-accounts%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%mozilla.org-firefox_home%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%send.firefox.com%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%addons.mozilla.org%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%premium.firefox.com%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%fpn.firefox.com%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%leanplum-push-qa%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%fx-ios-vpn%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%modal%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%fx-relay-addon%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%fx-relay%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%mozilla.org-firefox-browsers%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%new-tab-ad%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%firefox%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%pocket_saves%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%www.mozilla.org-vpn-or-proxy%'
      THEN 'Product Owned'
    WHEN utm_source LIKE '%fx-vpn-iOSs%'
      THEN 'Product Owned'
    -- Marketing Paid
    WHEN utm_source LIKE '%facebook%'
      OR utm_source LIKE '%instagram%'
      THEN 'Marketing Paid'
    WHEN utm_source LIKE '%google%'
      THEN 'Marketing Paid'
    WHEN utm_source = 'reddit'
      THEN 'Marketing Paid'
    WHEN utm_source = 'dv360'
      THEN 'Marketing Paid'
    WHEN utm_source LIKE '%saasworthy.com%'
      THEN 'Marketing Paid'
    WHEN utm_source LIKE '%youtube%'
      THEN 'Marketing Paid'
    -- Miscellaneous (catch-all for remaining patterns)
    WHEN utm_source LIKE '%relay%'
      THEN 'Miscellaneous'
    WHEN utm_source LIKE '%desktop-signup-flow%'
      THEN 'Miscellaneous'
    WHEN utm_source = 'invalid'
      THEN 'Miscellaneous'
    WHEN utm_source LIKE '%multi.account.containers%'
      THEN 'Miscellaneous'
    WHEN utm_source IN (
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
      THEN 'Marketing Owned'
    WHEN utm_source LIKE '%.mozilla.org%'
      THEN 'Marketing Owned'
    -- Final fallback
    ELSE 'Miscellaneous'
  END
);
