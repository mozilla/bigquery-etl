SELECT
  DATE(submission_timestamp) AS submission_date,
  environment.system.os.name AS os,
  metadata.geo.country,
  SUM(
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.http_pageload_is_ssl, '$.values.0') AS INT64)
  ) AS non_ssl_loads,
  SUM(
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.http_pageload_is_ssl, '$.values.1') AS INT64)
  ) AS ssl_loads,
  -- ratio of pings that have the probe
  COUNT(payload.histograms.http_pageload_is_ssl) / COUNT(*) AS reporting_ratio
FROM
  telemetry.main
WHERE
  sample_id = 42
  AND normalized_channel = 'release'
  AND environment.system.os.name IN ('Windows_NT', 'Darwin', 'Linux')
  AND application.name = 'Firefox'
  AND DATE(submission_timestamp) > DATE '2016-11-01'
  AND (DATE(submission_timestamp) = @submission_date OR @submission_date IS NULL)
GROUP BY
  submission_date,
  os,
  country

CREATE TEMPORARY FUNCTION get_client_ip(
  xff STRING, 
  remote_address STRING,
  pipeline_proxy STRING
)
RETURNS STRING
LANGUAGE js
AS
  """
  if (xff != null) {
    // Google's load balancer will append the immediate sending client IP and a global
    // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
    // https://cloud.google.com/load-balancing/docs/https/#components
    //
    // In practice, many of the "first" addresses are bogus or internal,
    // so we target the immediate sending client IP.
    ips = xff.split("\\s*,\\s*");

    if (pipeline_proxy != null) {
      ip = ips[ips.length - 1];
    } else {
      ip = ips[Math.max(ips.length - 2, 0)];
    }
  } else {
    ip = "";
    if (remote_address != null) {
      ip = remote_address;
    }
  }

  return ip;
  """;
--
CREATE TEMPORARY FUNCTION cidr_range(
  network STRING
)
RETURNS STRUCT<
  start_ip FLOAT64,
  end_ip FLOAT64>
LANGUAGE js
AS
  """
  function ipToInt(ipAddress) 
  {
    var d = ipAddress.split('.');
    return BigInt(((((((+d[0])*256)+(+d[1]))*256)+(+d[2]))*256)+(+d[3]));
  }

  function ipMask(maskSize) {
    return -1n << (32n - BigInt(parseInt(maskSize)));
  }

  var subnetIp = network.split("/")[0];
  var subnetMask = network.split("/")[1];
  var startIp = ipToInt(subnetIp) & ipMask(subnetMask);
  var endIp = startIp + BigInt(Math.pow(2, (32 - subnetMask))) - 1n;


  return {
    "start_ip": parseFloat(startIp),
    "end_ip": parseFloat(endIp)
  };
  """;
--
WITH 
asn_ip_address_range AS (
  SELECT 
    cidr_range(network) as ip_range,
    autonomous_system_number
  FROM 
    `static.geoip2_isp_blocks_ipv4`
),
main_summary_with_ip AS (
  SELECT 
    submission_date,
    client_id,
    user_pref_network_trr_mode,
    NET.IPV4_TO_INT64(NET.SAFE_IP_FROM_STRING(ip_address)),
  FROM `moz-fx-data-shar-nonprod-efed.telemetry.main_summary` as main_summary
  LEFT JOIN
    (SELECT 
      submission_timestamp,
      get_client_ip(x_forwarded_for, remote_addr, x_pipeline_proxy) as ip_address,
      udf.parse_desktop_telemetry_uri(uri).document_id AS document_id
    FROM `moz-fx-data-shar-nonprod-efed.payload_bytes_raw.telemetry`) AS payload_bytes_raw
  ON payload_bytes_raw.document_id = main_summary.document_id
  WHERE main_summary.submission_date = '2020-03-22' AND
  DATE(payload_bytes_raw.submission_timestamp) = '2020-03-22'
),
main_summary_with_asn AS (
  SELECT 
    submission_date,
    client_id,
    user_pref_network_trr_mode,
    (SELECT autonomous_system_number 
      FROM asn_ip_address_range 
      WHERE ip_range.start >= ip_address AND
      ip_range.end <= ip_address)
  FROM main_summary_with_ip
)
SELECT 
  submission_date,
  asn,
  COUNT(distinct client_id) AS n_clients,
  COUNT(user_pref_network_trr_mode > 0) as doh_enabled,
  COUNT(user_pref_network_trr_mode = 0) as doh_disabled
FROM main_summary_with_asn
GROUP BY 
  submission_date, 
  asn
HAVING n_clients  > 50
  
  

CREATE TEMPORARY FUNCTION get_client_ip(
  xff STRING, 
  remote_address STRING,
  pipeline_proxy STRING
)
RETURNS STRING
LANGUAGE js
AS
  """
  if (xff != null) {
    // Google's load balancer will append the immediate sending client IP and a global
    // forwarding rule IP to any existing content in X-Forwarded-For as documented in:
    // https://cloud.google.com/load-balancing/docs/https/#components
    //
    // In practice, many of the "first" addresses are bogus or internal,
    // so we target the immediate sending client IP.
    ips = xff.split("\\s*,\\s*");

    if (pipeline_proxy != null) {
      ip = ips[ips.length - 1];
    } else {
      ip = ips[Math.max(ips.length - 2, 0)];
    }
  } else {
    ip = "";
    if (remote_address != null) {
      ip = remote_address;
    }
  }

  return ip;
  """;
CREATE TEMPORARY FUNCTION cidr_range(
  network STRING
)
RETURNS STRUCT<
  start_ip FLOAT64,
  end_ip FLOAT64>
LANGUAGE js
AS
  """
  function ipToInt(ipAddress) 
  {
    var d = ipAddress.split('.');
    return BigInt(((((((+d[0])*256)+(+d[1]))*256)+(+d[2]))*256)+(+d[3]));
  }

  function ipMask(maskSize) {
    return -1n << (32n - BigInt(parseInt(maskSize)));
  }

  var subnetIp = network.split("/")[0];
  var subnetMask = network.split("/")[1];
  var startIp = ipToInt(subnetIp) & ipMask(subnetMask);
  var endIp = startIp + BigInt(Math.pow(2, (32 - subnetMask))) - 1n;


  return {
    "start_ip": parseFloat(startIp),
    "end_ip": parseFloat(endIp)
  };
  """;
--
WITH 
asn_ip_address_range AS (
  SELECT 
    cidr_range(network).start_ip as start_ip,
    cidr_range(network).end_ip as end_ip,
    autonomous_system_number
  FROM 
    `static.geoip2_isp_blocks_ipv4`
  ORDER BY start_ip ASC
),
main_summary_with_ip AS (
  SELECT 
    submission_date,
    client_id,
    user_pref_network_trr_mode as trr,
    NET.IPV4_TO_INT64(NET.SAFE_IP_FROM_STRING(ip_address)) as ip_address
  FROM `moz-fx-data-shared-prod.telemetry.main_summary` as main_summary
  LEFT JOIN
    (SELECT 
      submission_timestamp,
      get_client_ip(x_forwarded_for, remote_addr, x_pipeline_proxy) as ip_address,
      udf.parse_desktop_telemetry_uri(uri).document_id AS document_id
    FROM `moz-fx-data-shared-prod.payload_bytes_raw.telemetry`) AS payload_bytes_raw
  ON payload_bytes_raw.document_id = main_summary.document_id
  WHERE submission_date = '2020-03-20' AND
  DATE(payload_bytes_raw.submission_timestamp) = '2020-03-20'
  LIMIT 1000
),
-- main_summary_with_ip_grouped AS (
--   SELECT 
--   submission_date,
--   ip_address,
--   COUNT(distinct client_id) AS n_clients
-- --   COUNT(trr > 0) as doh_enabled,
-- --   COUNT(trr = 0) as doh_disabled
-- FROM main_summary_with_ip
-- GROUP BY 
--   submission_date, 
--   ip_address
-- )
main_summary_with_asn AS (
  SELECT 
    submission_date,
    client_id,
    trr,
    autonomous_system_number,
  FROM main_summary_with_ip
  INNER JOIN
    asn_ip_address_range
      ON main_summary_with_ip.ip_address BETWEEN start_ip AND end_ip
)
SELECT * from main_summary_with_asn