-- Override the default glean_usage generated view to union data from all VPN clients.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.deletion_request`
AS
SELECT
  * EXCEPT (metrics)
FROM
  `moz-fx-data-shared-prod.mozillavpn.deletion_request`
UNION ALL
SELECT
  * EXCEPT (metrics)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.deletion_request`
