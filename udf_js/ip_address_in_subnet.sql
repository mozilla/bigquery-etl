CREATE OR REPLACE FUNCTION udf_js.ip_address_in_network(
  ip_address STRING,
  network STRING
)
RETURNS BOOL
LANGUAGE js
AS
  """
  function ipToInt(ipAddress) 
  {
    var d = ipAddress.split('.');
    return ((((((+d[0])*256)+(+d[1]))*256)+(+d[2]))*256)+(+d[3]);
  }

  function ipMask(maskSize) {
    return -1 << (32 - maskSize);
  }

  var subnetIp = network.split("/")[0];
  var subnetMask = network.split("/")[1];
  return (ipToInt(ip_address) & ipMask(subnetMask)) == (ipToInt(subnetIp) & ipMask(subnetMask));
  """;

-- Test

SELECT
  assert_false(udf_js.ip_address_in_network("0.0.0.0", "1.1.1.1/8")),
  assert_true(udf_js.ip_address_in_network("1.1.1.1", "1.1.1.1/8")),
  assert_false(udf_js.ip_address_in_network("10.128.239.50", "10.128.240.50/30")),
  assert_true(udf_js.ip_address_in_network("10.128.240.50", "10.128.240.50/30")),
  assert_true(udf_js.ip_address_in_network("192.168.5.1", "192.168.5.85/24")),
  assert_false(udf_js.ip_address_in_network("192.168.4.254", "192.168.5.85/24")),
  assert_true(udf_js.ip_address_in_network("192.168.5.254", "192.168.5.85/24"))


CREATE OR REPLACE FUNCTION udf_js.cidr_range(
  network STRING
)
RETURNS STRUCT<
  start_ip INT64,
  end_ip INT64>
LANGUAGE js
AS
  """
  function ipToInt(ipAddress) 
  {
    var d = ipAddress.split('.');
    return ((((((+d[0])*256)+(+d[1]))*256)+(+d[2]))*256)+(+d[3]);
  }

  function ipMask(maskSize) {
    return -1 << (32 - maskSize);
  }

  var subnetIp = network.split("/")[0];
  var subnetMask = network.split("/")[1];
  var startIp = ipToInt(subnetIp) & ipMask(subnetMask);
  var endIp = startIp + Math.pow(2, (32 - subnetMask)) - 1;

  return {
    "start_ip": startIp,
    "end_ip": endIp
  };
  """;
