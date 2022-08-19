-- Override the default glean_usage generated view to union data from all VPN clients.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.events`
AS
-- Data from VPN clients using Glean.js
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozillavpn.events`
UNION ALL
-- Data from VPN Android clients using Glean Kotlin SDK
SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.events`
