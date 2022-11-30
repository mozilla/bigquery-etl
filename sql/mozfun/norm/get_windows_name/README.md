# Windows Name, Version, and Build Number

### Summary
This function is designed to parse the field `os_version` in table `mozdata.default_browser_agent.default_browser` and is intended to be used as part of the suite of functions `get_windows_name`, `get_windows_version`, and `get_windows_build_number`. 

As of November 2022, the expected valid values of `os_version` are either `x.y.z` or `w.x.y.z` where `w`, `x`, `y`, and `z` are integers. However, the database does contain other invalid values.

As of November 22, the return values are based on [Windows 10 release information](https://learn.microsoft.com/en-us/windows/release-health/release-information) and [Windows 11 release information](https://learn.microsoft.com/en-us/windows/release-health/windows11-release-information).

Note: Microsoft convention for build numbers for Windows 10 and 11 include two numbers. The first number is the version number and the second number uniquely identifies the build within the version. Example: `22621.900`. To simplify data processing and data analysis, `get_windows_build_number` returns the second unique identifier as an integer instead of returning the full build number as a string.

### Example usage

```sql
SELECT
	 `os_version`,
	 mozfun.norm.get_windows_name(`os_version`),
	 mozfun.norm.get_windows_version(`os_version`),
	 mozfun.norm.get_windows_build_number(`os_version`)
FROM `mozdata.default_browser_agent.default_browser`
WHERE `submission_timestamp` > (CURRENT_TIMESTAMP() - INTERVAL 10 DAY)
```

### Mapping
os_version       | windows_name  | windows_version | windows_build_number
---------------- | ------------- | --------------- | --------------------
6.0.z            | Windows Vista | 6.0             | z
6.1.z            | Windows 7     | 7.0             | z
6.2.z            | Windows 8     | 8.0             | z
6.3.z            | Windows 8.1   | 8.1             | z
10.0.10240.z     | Windows 10    | 1507            | z
10.0.10586.z     | Windows 10    | 1511            | z
10.0.14393.z     | Windows 10    | 1607            | z
10.0.15063.z     | Windows 10    | 1703            | z
10.0.16299.z     | Windows 10    | 1709            | z
10.0.17134.z     | Windows 10    | 1803            | z
10.0.17763.z     | Windows 10    | 1809            | z
10.0.18362.z     | Windows 10    | 1903            | z
10.0.18363.z     | Windows 10    | 1909            | z
10.0.19041.z     | Windows 10    | 2004            | z
10.0.19042.z     | Windows 10    | 20H2            | z
10.0.19043.z     | Windows 10    | 21H1            | z
10.0.19044.z     | Windows 10    | 21H2            | z
10.0.19045.z     | Windows 10    | 22H2            | z
10.0.22000.z     | Windows 11    | 21H2            | z
10.0.22621.z     | Windows 11    | 22H2            | z
all other values | (null)        | (null)          | (null)
