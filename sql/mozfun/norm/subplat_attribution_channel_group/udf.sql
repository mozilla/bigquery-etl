CREATE OR REPLACE FUNCTION norm.subplat_attribution_channel_group(utm_source STRING)
RETURNS STRING AS (
  CASE
    -- Marketing Owned
    WHEN utm_source LIKE ANY(
        '%blog.mozilla.org%',
        '%firefox-desktop%',
        '%fxaonboardingemail%',
        '%fxakip%',
        '%fxatips%',
        '%fxavpn%',
        '%fxnews%',
        '%instagram%',
        '%invite%',
        '%mozilla.org-whatsnew%',
        '%mozilla.org-welcome%',
        '%oim%',
        '%pkt-hits%',
        '%pockethits%',
        '%relay-onboarding%',
        '%stage.fxprivaterelay.nonprod.cloudops.mozgcp.net%',
        '%sync-onboarding%',
        '%twitter.com%',
        '%vpnwaitlist%',
        '%wrapped_email%'
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
        '%about-preferences%',
        '%about-prefs%',
        '%accounts.firefox.com%',
        '%activity-stream%',
        '%addons.mozilla.org%',
        '%firefox-browser%',
        '%fpn.firefox.com%',
        '%fx-ios-vpn%',
        '%fx-monitor%',
        '%fx-relay%',
        '%fx-relay-addon%',
        '%fx-vpn-iOSs%',
        '%fx-vpn-windows%',
        '%leanplum-push-notification%',
        '%leanplum-push-qa%',
        '%modal%',
        '%monitor.firefox.com%',
        '%mozilla.org-firefox-accounts%',
        '%mozilla.org-firefox-browsers%',
        '%mozilla.org-firefox_home%',
        '%new-tab-ad%',
        '%newtab%',
        '%pocket%',
        '%pocket_saves%',
        '%premium.firefox.com%',
        '%privatebrowser%',
        '%relay-firefox-com.translate.goog%',
        '%send.firefox.com%',
        '%sponsoredtile%',
        '%spotlight-modal%',
        '%thunderbird%',
        '%toolbar%',
        '%www.mozilla.org-vpn-or-proxy%'
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
    -- Default for any unmapped values (based on the data, this should be Marketing Owned for most mozilla domains)
    WHEN utm_source LIKE '%mozilla.org%'
      THEN 'Marketing Owned'
    -- Final fallback
    ELSE 'Miscellaneous'
  END
);
