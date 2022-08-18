-- Override the default glean_usage generated view to union data from all VPN clients.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.deletion_request`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozillavpn.deletion_request`
UNION ALL
SELECT
  * REPLACE (
    STRUCT(
      CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS jwe,
      metrics.labeled_counter AS labeled_counter,
      CAST(
        NULL
        AS
          ARRAY<
            STRUCT<
              key STRING,
              value ARRAY<STRUCT<key STRING, value STRUCT<denominator INT64, numerator INT64>>>
            >
          >
      ) AS labeled_rate,
      CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS url,
      CAST(NULL AS ARRAY<STRUCT<key STRING, value STRING>>) AS text
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.deletion_request`
