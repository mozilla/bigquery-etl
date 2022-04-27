Note: Non-zero minor and patch versions will be floating point `Numeric`.

Usage:

```sql
SELECT
    mozfun.norm.extract_version(version_string, 'major') as major_version,
    mozfun.norm.extract_version(version_string, 'minor') as minor_version,
    mozfun.norm.extract_version(version_string, 'patch') as patch_version
```

Example using `"96.05.01"`:

```sql
SELECT
    mozfun.norm.extract_version('96.05.01', 'major') as major_version, -- 96
    mozfun.norm.extract_version('96.05.01', 'minor') as minor_version, -- .05
    mozfun.norm.extract_version('96.05.01', 'patch') as patch_version  -- .01
```