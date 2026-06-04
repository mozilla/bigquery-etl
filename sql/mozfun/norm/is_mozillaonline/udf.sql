CREATE OR REPLACE FUNCTION norm.is_mozillaonline(distribution_id STRING, app_version STRING)
RETURNS BOOL AS (
  -- MozillaOnline migration shipped in 151.0.3 (June 2, 2026).
  -- Users on 151.0.3+ reporting distribution_id = 'MozillaOnline' are
  -- post-migration canonical Firefox users; treat them as non-MozillaOnline.
  CASE
    WHEN distribution_id IS NULL
      OR app_version IS NULL
      THEN FALSE
    WHEN LOWER(distribution_id) != 'mozillaonline'
      THEN FALSE
    ELSE IFNULL(
        norm.browser_version_info(app_version).major_version < 151
        OR (
          norm.browser_version_info(app_version).major_version = 151
          AND norm.browser_version_info(app_version).minor_version = 0
          AND norm.browser_version_info(app_version).patch_revision < 3
        )
        OR (
          norm.browser_version_info(app_version).major_version = 151
          AND norm.browser_version_info(app_version).minor_version = 0
          AND norm.browser_version_info(app_version).patch_revision IS NULL
        ),
        TRUE
      )
  END
);

-- Tests
SELECT
  -- Pre-migration: TRUE
  assert.true(norm.is_mozillaonline('MozillaOnline', '151.0.2')),
  -- Exactly the migration version: FALSE
  assert.false(norm.is_mozillaonline('MozillaOnline', '151.0.3')),
  -- Post-migration: FALSE
  assert.false(norm.is_mozillaonline('MozillaOnline', '152.0')),
  -- Well before migration: TRUE
  assert.true(norm.is_mozillaonline('MozillaOnline', '115.0')),
  -- Case-insensitive distribution_id: TRUE
  assert.true(norm.is_mozillaonline('MOZILLAONLINE', '151.0.2')),
  -- Canonical Firefox distribution: FALSE
  assert.false(norm.is_mozillaonline('Firefox', '151.0.2')),
  -- NULL distribution_id: FALSE
  assert.false(norm.is_mozillaonline(NULL, '151.0.2')),
  -- NULL app_version: FALSE
  assert.false(norm.is_mozillaonline('MozillaOnline', NULL)),
  -- Both NULL: FALSE
  assert.false(norm.is_mozillaonline(NULL, NULL)),
  -- Malformed app_version + MozillaOnline: TRUE (conservative fallback)
  assert.true(norm.is_mozillaonline('MozillaOnline', 'foo-bar')),
  -- Empty string distribution_id: FALSE
  assert.false(norm.is_mozillaonline('', '151.0.2'));
