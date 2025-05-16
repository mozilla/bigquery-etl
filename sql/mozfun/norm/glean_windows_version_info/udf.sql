CREATE OR REPLACE FUNCTION norm.glean_windows_version_info(
  os STRING,
  os_version STRING,
  windows_build_number INT64
)
RETURNS STRING AS (
  CASE
    WHEN os = 'Windows'
      AND os_version = '4.0'
      THEN 'Windows NT 4.0'
    WHEN os = 'Windows'
      AND os_version = '4.1'
      THEN 'Windows 98'
    WHEN os = 'Windows'
      AND os_version = '5.0'
      THEN 'Windows 2000'
    WHEN os = 'Windows'
      AND os_version IN ('5.1', '5.2')
      THEN 'Windows XP'
    WHEN os = 'Windows'
      AND os_version = '6.0'
      THEN 'Windows Vista'
    WHEN os = 'Windows'
      AND os_version = '6.1'
      THEN 'Windows 7'
    WHEN os = 'Windows'
      AND os_version = '6.2'
      THEN 'Windows 8'
    WHEN os = 'Windows'
      AND os_version = '6.3'
      THEN 'Windows 8.1'
    WHEN os = 'Windows'
      AND os_version = '10.0'
      AND windows_build_number < 22000
      THEN 'Windows 10'
    WHEN os = 'Windows'
      AND os_version = '10.0'
      AND windows_build_number >= 22000
      THEN 'Windows 11'
    WHEN os = 'Windows'
      AND os_version = '10.0'
      AND windows_build_number IS NULL
      THEN 'Windows 10/11 (build unknown)'
    ELSE NULL
  END
);

-- Tests
SELECT
  assert.equals('Windows NT 4.0', norm.glean_windows_version_info('Windows', '4.0', NULL)),
  assert.equals('Windows 98', norm.glean_windows_version_info('Windows', '4.1', NULL)),
  assert.equals('Windows 2000', norm.glean_windows_version_info('Windows', '5.0', NULL)),
  assert.equals('Windows XP', norm.glean_windows_version_info('Windows', '5.1', NULL)),
  assert.equals('Windows XP', norm.glean_windows_version_info('Windows', '5.2', NULL)),
  assert.equals('Windows Vista', norm.glean_windows_version_info('Windows', '6.0', NULL)),
  assert.equals('Windows 7', norm.glean_windows_version_info('Windows', '6.1', 7601)),
  assert.equals('Windows 8', norm.glean_windows_version_info('Windows', '6.2', 7602)),
  assert.equals('Windows 8.1', norm.glean_windows_version_info('Windows', '6.3', NULL)),
  assert.equals('Windows 10', norm.glean_windows_version_info('Windows', '10.0', 19043)),
  assert.equals('Windows 11', norm.glean_windows_version_info('Windows', '10.0', 22623)),
  assert.equals(
    'Windows 10/11 (build unknown)',
    norm.glean_windows_version_info('Windows', '10.0', NULL)
  );
