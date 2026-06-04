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
        (
          SELECT
            v.major_version < 151
            OR (
              v.major_version = 151
              AND v.minor_version = 0
              AND (v.patch_revision < 3 OR v.patch_revision IS NULL)
            )
          FROM
            UNNEST([norm.browser_version_info(app_version)]) AS v
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
  -- Major release without patch (151.0): TRUE (pre-migration, patch is NULL)
  assert.true(norm.is_mozillaonline('MozillaOnline', '151.0')),
  -- Minor version bump (151.1): FALSE (post-migration)
  assert.false(norm.is_mozillaonline('MozillaOnline', '151.1')),
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
