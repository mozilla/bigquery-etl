
-- SELECT autonomous_system_number, network
-- FROM `static.geoip2_isp_blocks_ipv4`
-- WHERE NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING("100.1.0.255"), CAST(SPLIT(network, "/")[OFFSET(1)] AS INT64)) = NET.SAFE_IP_FROM_STRING(SPLIT(network, "/")[OFFSET(0)])
CREATE TEMPORARY FUNCTION get_client_ip(xff STRING, remote_address STRING, pipeline_proxy STRING)
RETURNS STRING DETERMINISTIC
LANGUAGE js
AS
  """
  // X-Forwarded-For is a list of IP addresses
  if (xff != null) {
    // Google's load balancer will append the immediate sending client IP and a global
    // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
    // https://cloud.google.com/load-balancing/docs/https/#components
    //
    // In practice, many of the "first" addresses are bogus or internal,
    // so we target the immediate sending client IP.
    ips = xff.split(",");

    if (pipeline_proxy != null) {
      // Drop extra IP from X-Forwarded-For
      ips = ips.slice(0, -1);
    }

    ip = ips[Math.max(ips.length - 2, 0)].trim();
  } else {
    ip = "";
    if (remote_address != null) {
      ip = remote_address;
    }
  }

  return ip;
  """;

--
WITH asn_ip_address_range AS (
  SELECT
    NET.SAFE_IP_FROM_STRING(SPLIT(network, "/")[OFFSET(0)]) AS network_ip,
    CAST(SPLIT(network, "/")[OFFSET(1)] AS INT64) AS mask,
    autonomous_system_number
  FROM
    `moz-fx-data-shared-prod.static.isp_blocks_ipv4_20200407`
),
events_with_doh AS (
  -- Get event data with DoH information.
  SELECT
    submission_date,
    document_id,
    client_id,
    event_object,
    event_category,
    udf.get_key(event_map_values, 'canary') AS canary,
  FROM
    `moz-fx-data-shared-prod.telemetry.events` AS events
  WHERE
    submission_date = @submission_date
),
events_with_ip AS (
  -- Get IP addresses and client data.
  SELECT
    submission_date,
    client_id,
    canary,
    event_category,
    event_object,
    NET.SAFE_IP_FROM_STRING(ip_address) AS ip_address
  FROM
    events_with_doh AS events
  LEFT JOIN
    (
      SELECT
        submission_timestamp,
        get_client_ip(x_forwarded_for, remote_addr, x_pipeline_proxy) AS ip_address,
        udf.parse_desktop_telemetry_uri(uri).document_id AS document_id
      FROM
        `moz-fx-data-shared-prod.payload_bytes_raw.telemetry`
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) AS payload_bytes_raw
  ON
    payload_bytes_raw.document_id = events.document_id
),
events_with_asn AS (
  -- Lookup ASNs for IP addresses.
  SELECT DISTINCT
    submission_date,
    client_id,
    canary,
    event_category,
    event_object,
    autonomous_system_number
  FROM
    (
      SELECT
        *,
        ip_address & NET.IP_NET_MASK(4, mask) network_ip
      FROM
        events_with_ip,
        UNNEST(GENERATE_ARRAY(9, 32)) mask
      WHERE
        BYTE_LENGTH(ip_address) = 4
    )
  JOIN
    asn_ip_address_range
  USING
    (network_ip, mask)
)
SELECT
  submission_date,
  autonomous_system_number,
  COUNT(DISTINCT client_id) AS n_clients,
  COUNTIF(
    event_category LIKE 'doh'
    AND event_object LIKE 'heuristics'
    AND canary LIKE 'enable_doh'
  ) AS doh_enabled,
  COUNTIF(
    event_category LIKE 'doh'
    AND event_object LIKE 'heuristics'
    AND canary LIKE 'disable_doh'
  ) AS doh_disabled
FROM
  events_with_asn
GROUP BY
  submission_date,
  autonomous_system_number
HAVING
  n_clients > @n_clients
