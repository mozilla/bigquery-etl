CREATE OR REPLACE FUNCTION norm.get_windows_info(os_version STRING)
RETURNS STRUCT<name STRING, version_name STRING, version_number DECIMAL, build_number INT64>
LANGUAGE js AS r"""
  const windows_8_or_earlier_dict = {
    0: {name: "Windows Vista", version_name: "6", version_number: 6.0},
    1: {name: "Windows 7", version_name: "7", version_number: 6.1},
    2: {name: "Windows 8", version_name: "8", version_number: 6.2},
    3: {name: "Windows 8.1", version_name: "8.1", version_number: 6.3},
  };
  const windows_10_or_later_dict = {
    10240: {name: "Windows 10", version_name: "1507", version_number: 10240},
    10586: {name: "Windows 10", version_name: "1511", version_number: 10586},
    14393: {name: "Windows 10", version_name: "1607", version_number: 14393},
    15063: {name: "Windows 10", version_name: "1703", version_number: 15063},
    16299: {name: "Windows 10", version_name: "1709", version_number: 16299},
    17134: {name: "Windows 10", version_name: "1803", version_number: 17134},
    17763: {name: "Windows 10", version_name: "1809", version_number: 17763},
    18362: {name: "Windows 10", version_name: "1903", version_number: 18362},
    18363: {name: "Windows 10", version_name: "1909", version_number: 18363},
    19041: {name: "Windows 10", version_name: "2004", version_number: 19041},
    19042: {name: "Windows 10", version_name: "20H2", version_number: 19042},
    19043: {name: "Windows 10", version_name: "21H1", version_number: 19043},
    19044: {name: "Windows 10", version_name: "21H2", version_number: 19044},
    19045: {name: "Windows 10", version_name: "22H2", version_number: 19045},
    22000: {name: "Windows 11", version_name: "21H2", version_number: 22000},
    22621: {name: "Windows 11", version_name: "22H2", version_number: 22621},
  };

  // Valid values of os_version contain either 3 or 4 dot-separated numbers
  // denoted as w.x.y.z or x.y.z below.
  const fields = os_version.split(".");

  // Parse values for 10.0.y.z where z is 5 digits or shorter.
  if (fields.length == 4) {
    const w = parseInt(fields[0]);
    if (w == 10) {
      const x = parseInt(fields[1]);
      if (x == 0) {
        const z = parseInt(fields[3]);
        if (0 <= z && z < 100000) {
          const y = parseInt(fields[2]);
          if (y in windows_10_or_later_dict) {
            const info = windows_10_or_later_dict[y];
            return {
              name: info["name"],
              version_name: info["version_name"],
              version_number: info["version_number"],
              build_number: z
            };
          } else {
            if (0 <= y && y < 22000) {
              return {
                name: "Windows 10",
                version_name: "UNKNOWN",
                version_number: y,
                build_number: z,
              };
            } else if (22000 <= y) {
              return {
                name: "Windows 11",
                version_name: "UNKNOWN",
                version_number: y,
                build_number: z,
              };
            }
          }
        }
      }
    }

  // Parse values for 6.y.z where y is known and z is 4 digits or shorter.
  } else if (fields.length == 3) {
    const x = parseInt(fields[0]);
    if (x == 6) {
      const z = parseInt(fields[2]);
      if (0 <= z && z < 10000) {
        const y = parseInt(fields[1]);
        if (y in windows_8_or_earlier_dict) {
          const info = windows_8_or_earlier_dict[y];
          return {
            name: info["name"],
            version_name: info["version_name"],
            version_number: info["version_number"],
            build_number: z
          };
        }
      }
    }
  }
  return null;
""";

-- Tests
SELECT
  assert.null(norm.get_windows_info("non-numeric")),
  assert.null(norm.get_windows_info("non.numeric")),
  assert.null(norm.get_windows_info("non.numeric.values")),
  assert.null(norm.get_windows_info("7")),
  assert.null(norm.get_windows_info("6.1")),
  assert.null(norm.get_windows_info("6.2.13600")),
  assert.null(norm.get_windows_info("6.3.-5")),
  assert.null(norm.get_windows_info("6.8.11")),
  assert.null(norm.get_windows_info("6.10.67")),
  assert.null(norm.get_windows_info("8.3.8601")),
  assert.null(norm.get_windows_info("105.0.0")),
  assert.null(norm.get_windows_info("10.0.18363.4050523339")),
  assert.null(norm.get_windows_info("10.0.0.-15")),
  assert.null(norm.get_windows_info("10.0.-7.0")),
  assert.null(norm.get_windows_info("10.1.0.0")),
  assert.null(norm.get_windows_info("85456040.85456040.85456040.85456040")),
  assert.equals("Windows Vista", norm.get_windows_info("6.0.6002").name),
  assert.equals("6", norm.get_windows_info("6.0.6002").version_name),
  assert.equals(6.0, norm.get_windows_info("6.0.6002").version_number),
  assert.equals(6002, norm.get_windows_info("6.0.6002").build_number),
  assert.equals("Windows 7", norm.get_windows_info("6.1.7077").name),
  assert.equals("7", norm.get_windows_info("6.1.7077").version_name),
  assert.equals(6.1, norm.get_windows_info("6.1.7077").version_number),
  assert.equals(7077, norm.get_windows_info("6.1.7077").build_number),
  assert.equals("Windows 8", norm.get_windows_info("6.2.9200").name),
  assert.equals("8", norm.get_windows_info("6.2.9200").version_name),
  assert.equals(6.2, norm.get_windows_info("6.2.9200").version_number),
  assert.equals(9200, norm.get_windows_info("6.2.9200").build_number),
  assert.equals("Windows 8.1", norm.get_windows_info("6.3.9431").name),
  assert.equals("8.1", norm.get_windows_info("6.3.9431").version_name),
  assert.equals(6.3, norm.get_windows_info("6.3.9431").version_number),
  assert.equals(9431, norm.get_windows_info("6.3.9431").build_number),
  assert.equals("Windows 10", norm.get_windows_info("10.0.10240.17443").name),
  assert.equals("1507", norm.get_windows_info("10.0.10240.17443").version_name),
  assert.equals(10240, norm.get_windows_info("10.0.10240.17443").version_number),
  assert.equals(17443, norm.get_windows_info("10.0.10240.17443").build_number),
  assert.equals("Windows 10", norm.get_windows_info("10.0.20270.1").name),
  assert.equals("UNKNOWN", norm.get_windows_info("10.0.20270.1").version_name),
  assert.equals(20270, norm.get_windows_info("10.0.20270.1").version_number),
  assert.equals(1, norm.get_windows_info("10.0.20270.1").build_number),
  assert.equals("Windows 11", norm.get_windows_info("10.0.22621.819").name),
  assert.equals("22H2", norm.get_windows_info("10.0.22621.819").version_name),
  assert.equals(22621, norm.get_windows_info("10.0.22621.819").version_number),
  assert.equals(819, norm.get_windows_info("10.0.22621.819").build_number),
  assert.equals("Windows 11", norm.get_windows_info("10.0.25145.1011").name),
  assert.equals("UNKNOWN", norm.get_windows_info("10.0.25145.1011").version_name),
  assert.equals(25145, norm.get_windows_info("10.0.25145.1011").version_number),
  assert.equals(1011, norm.get_windows_info("10.0.25145.1011").build_number),
