CREATE OR REPLACE FUNCTION norm.is_mozillaonline(distribution_id STRING, app_version STRING)
RETURNS BOOL AS (
  -- MozillaOnline migration thresholds: release 151.0.3, ESR 140: 140.12.0, ESR 115: 115.37.0.
  -- Users at or above these versions are post-migration; treat as non-MozillaOnline.
  CASE
    WHEN distribution_id IS NULL
      OR app_version IS NULL
      THEN FALSE
    WHEN LOWER(distribution_id) != 'mozillaonline'
      THEN FALSE
    ELSE IFNULL(
        (
          SELECT
            CASE
              -- Malformed version: fall through to IFNULL → TRUE
              WHEN v.major_version IS NULL
                THEN NULL
              -- ESR 115 migration: 115.37.0+ (release 115 topped at 115.0.3)
              WHEN v.major_version = 115
                AND v.minor_version >= 37
                THEN FALSE
              -- ESR 140 migration: 140.12.0+ (release 140 topped at 140.0.4)
              WHEN v.major_version = 140
                AND v.minor_version >= 12
                THEN FALSE
              -- Pre-release-migration: all versions below 151
              WHEN v.major_version < 151
                THEN TRUE
              -- Release migration boundary: 151.0.0–151.0.2 are pre-migration
              WHEN v.major_version = 151
                AND v.minor_version = 0
                AND (v.patch_revision < 3 OR v.patch_revision IS NULL)
                THEN TRUE
              -- 151.0.3+: post-migration
              ELSE FALSE
            END
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
  -- ESR 115 at migration version: FALSE
  assert.false(norm.is_mozillaonline('MozillaOnline', '115.37.0')),
  -- ESR 115 pre-migration: TRUE
  assert.true(norm.is_mozillaonline('MozillaOnline', '115.36.0')),
  -- ESR 115 post-migration: FALSE
  assert.false(norm.is_mozillaonline('MozillaOnline', '115.38.0')),
  -- ESR 140 at migration version: FALSE
  assert.false(norm.is_mozillaonline('MozillaOnline', '140.12.0')),
  -- ESR 140 pre-migration: TRUE
  assert.true(norm.is_mozillaonline('MozillaOnline', '140.11.0')),
  -- ESR 140 post-migration: FALSE
  assert.false(norm.is_mozillaonline('MozillaOnline', '140.13.0')),
  -- Release 115 (not ESR): TRUE
  assert.true(norm.is_mozillaonline('MozillaOnline', '115.0.3')),
  -- Release 140 (not ESR): TRUE
  assert.true(norm.is_mozillaonline('MozillaOnline', '140.0.4')),
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
