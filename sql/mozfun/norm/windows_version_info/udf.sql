CREATE OR REPLACE FUNCTION norm.windows_version_info(os STRING, os_version STRING, windows_build_number INT64)
RETURNS STRING AS (
  CASE
    WHEN os NOT IN ('Windows_NT', 'Windows_95', 'Windows_98') THEN NULL
    WHEN os = 'Windows_95' THEN 'Windows 95'
    WHEN os = 'Windows_98' THEN 'Windows 98'
    WHEN os_version = '6.1' THEN 'Windows 7'
    WHEN os_version = '6.2' THEN 'Windows 8'
    WHEN os_version = '6.0' THEN 'Windows Vista'
    WHEN os_version = '5.1' OR os_version = '5.2' THEN 'Windows XP'
    WHEN os_version = '10.0' and windows_build_number >= 22000 THEN 'Windows 11'
    WHEN os_version = '10.0' AND windows_build_number < 22000 THEN 'Windows 10'
    WHEN os_version = '4.0' AND os = 'Windows_NT' THEN 'Windows NT 4.0'
    WHEN os_version = '5.0' THEN 'Windows 2000'
    ELSE NULL
  END
);

-- Tests
SELECT
  assert.equals('Windows 10', norm.windows_version_info('Windows_NT', '10.0', 19043)),
  assert.equals(NULL, norm.windows_version_info('Darwin', '11.4.2', NULL)),
  assert.null('Windows 11', norm.windows_version_info('Windows_NT', '10.0', 22623)),
  assert.equals('Windows 7', norm.windows_version_info('Windows_NT', '6.1', 7601)),
  assert.equals('Windows_95', norm.windows_version_info('Windows_95', NULL, NULL))
