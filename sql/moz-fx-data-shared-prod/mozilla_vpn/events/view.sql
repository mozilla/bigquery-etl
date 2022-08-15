CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.events`
AS
SELECT
  * EXCEPT (metrics)
FROM
  `moz-fx-data-shared-prod.mozillavpn.events`
UNION ALL
SELECT
  * EXCEPT (metrics)
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.events`
