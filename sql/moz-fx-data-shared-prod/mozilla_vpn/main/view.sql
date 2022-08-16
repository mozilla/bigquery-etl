-- Override the default glean_usage generated view to union data from all VPN clients.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.main`
AS
SELECT
  * EXCEPT (metrics)
FROM
  `moz-fx-data-shared-prod.mozillavpn.main`
UNION ALL
SELECT
  * EXCEPT (metrics)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.main`
