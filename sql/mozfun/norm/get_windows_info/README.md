### Windows Names, Versions, and Builds

#### Summary
This function is primarily designed to parse the field `os_version` in table `mozdata.default_browser_agent.default_browser`. Given a Microsoft Windows OS version string, the function returns the name of the operating system, the version name, the version number, and the build number corresponding to the operating system. As of November 2022, the parser can handle 99.89% of the `os_version` values collected in table `mozdata.default_browser_agent.default_browser`.

##### Status as of November 2022
As of November 2022, the expected valid values of `os_version` are either `x.y.z` or `w.x.y.z` where `w`, `x`, `y`, and `z` are integers.

As of November 2022, the return values for Windows 10 and Windows 11 are based on [Windows 10 release information](https://learn.microsoft.com/en-us/windows/release-health/release-information) and [Windows 11 release information](https://learn.microsoft.com/en-us/windows/release-health/windows11-release-information). For 3-number version strings, the parser assumes the valid values of `z` in `x.y.z` are at most 5 digits in length. For 4-number version strings, the parser assumes the valid values of `z` in `w.x.y.z` are at most 6 digits in length. The function makes an educated effort to handle Windows Vista, Windows 7, Windows 8, and Windows 8.1 information, but does not guarantee the return values are absolutely accurate. The function assumes the presence of undocumented non-release versions of Windows 10 and Windows 11, and will return an estimated name, version number, build number but not the version name. The function does not handle other versions of Windows.

As of November 2022, the parser currently handles just over 99.89% of data in the field `os_version` in table `mozdata.default_browser_agent.default_browser`.

##### Build number conventions
Note: Microsoft convention for build numbers for Windows 10 and 11 include two numbers, such as build number `22621.900` for version `22621`. The first number repeats the version number and the second number uniquely identifies the build within the version. To simplify data processing and data analysis, this function returns the second unique identifier as an integer instead of returning the full build number as a string.

#### Example usage
```sql
SELECT
  `os_version`,
  mozfun.norm.get_windows_info(`os_version`) AS windows_info
FROM `mozdata.default_browser_agent.default_browser`
WHERE `submission_timestamp` > (CURRENT_TIMESTAMP() - INTERVAL 7 DAY) AND LEFT(document_id, 2) = '00'
LIMIT 1000
```

#### Mapping
os_version       | windows_name  | windows_version_name | windows_version_number | windows_build_number
---------------- | ------------- | -------------------- | ---------------------- | --------------------
6.0.z            | Windows Vista | 6.0                  | 6.0                    | z
6.1.z            | Windows 7     | 7.0                  | 6.1                    | z
6.2.z            | Windows 8     | 8.0                  | 6.2                    | z
6.3.z            | Windows 8.1   | 8.1                  | 6.3                    | z
10.0.10240.z     | Windows 10    | 1507                 | 10240                  | z
10.0.10586.z     | Windows 10    | 1511                 | 10586                  | z
10.0.14393.z     | Windows 10    | 1607                 | 14393                  | z
10.0.15063.z     | Windows 10    | 1703                 | 15063                  | z
10.0.16299.z     | Windows 10    | 1709                 | 16299                  | z
10.0.17134.z     | Windows 10    | 1803                 | 17134                  | z
10.0.17763.z     | Windows 10    | 1809                 | 17763                  | z
10.0.18362.z     | Windows 10    | 1903                 | 18362                  | z
10.0.18363.z     | Windows 10    | 1909                 | 18363                  | z
10.0.19041.z     | Windows 10    | 2004                 | 19041                  | z
10.0.19042.z     | Windows 10    | 20H2                 | 19042                  | z
10.0.19043.z     | Windows 10    | 21H1                 | 19043                  | z
10.0.19044.z     | Windows 10    | 21H2                 | 19044                  | z
10.0.19045.z     | Windows 10    | 22H2                 | 19045                  | z
10.0.y.z         | Windows 10    | UNKNOWN              | y                      | z
10.0.22000.z     | Windows 11    | 21H2                 | 22000                  | z
10.0.22621.z     | Windows 11    | 22H2                 | 22621                  | z
10.0.y.z         | Windows 11    | UNKNOWN              | y                      | z
all other values | (null)        | (null)               | (null)                 | (null)
