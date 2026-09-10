-- Identifies Firefox Desktop profiles created by the automated segment described in DENG-11590.
--
-- Signature verified against telemetry_derived.clients_first_seen_v3 on 2026-09-08: Linux
-- containers running a pinned Firefox 151.0, launched with the `-profile` argument (which
-- surfaces as startup_profile_selection_reason = 'argument-profile'). Supporting evidence:
-- 100% en-US locale, no attribution of any kind, 99.98% report only the new_profile ping,
-- and a 0.016% day-1 return rate versus 11.9% for legitimate Linux `-profile` profiles.
--
-- The version pin is what makes this precise. There is a stable ~20k/day legitimate Linux
-- `argument-profile` baseline on other app_versions that must NOT be caught, so filtering on
-- startup_profile_selection_reason alone is not safe. The date lower bound is 2026-08-27, the
-- first day the segment is distinguishable above noise (411 profiles, versus 22-29/day before).
--
-- CAVEAT: this predicate will begin over-catching once real Linux users roll onto 151.0, since
-- legitimate `-profile` users on that version become indistinguishable from the segment.
-- Revisit when 151.0 reaches broad release on Linux.
CREATE OR REPLACE FUNCTION udf.is_desktop_argument_profile_automation(
  normalized_os STRING,
  app_version STRING,
  startup_profile_selection_reason STRING,
  first_seen_date DATE
)
RETURNS BOOLEAN AS (
  COALESCE(
    normalized_os = 'Linux'
    AND app_version = '151.0'
    AND startup_profile_selection_reason = 'argument-profile'
    AND first_seen_date >= DATE '2026-08-27',
    FALSE
  )
);

SELECT
  -- The DENG-11590 segment.
  mozfun.assert.equals(
    TRUE,
    udf.is_desktop_argument_profile_automation(
      'Linux',
      '151.0',
      'argument-profile',
      DATE '2026-09-06'
    )
  ),
  -- Legitimate Linux `-profile` users on other versions are the ~20k/day baseline.
  mozfun.assert.equals(
    FALSE,
    udf.is_desktop_argument_profile_automation(
      'Linux',
      '140.11.0',
      'argument-profile',
      DATE '2026-09-06'
    )
  ),
  -- 151.0 without the `-profile` argument is an ordinary new profile.
  mozfun.assert.equals(
    FALSE,
    udf.is_desktop_argument_profile_automation(
      'Linux',
      '151.0',
      'firstrun-created-default',
      DATE '2026-09-06'
    )
  ),
  -- The segment is Linux-only; Windows/Mac 151.0 is unaffected.
  mozfun.assert.equals(
    FALSE,
    udf.is_desktop_argument_profile_automation(
      'Windows',
      '151.0',
      'argument-profile',
      DATE '2026-09-06'
    )
  ),
  -- Before onset, do not rewrite history.
  mozfun.assert.equals(
    FALSE,
    udf.is_desktop_argument_profile_automation(
      'Linux',
      '151.0',
      'argument-profile',
      DATE '2026-08-26'
    )
  ),
  -- NULLs must not propagate; callers filter on `NOT is_desktop_argument_profile_automation(...)`.
  mozfun.assert.equals(
    FALSE,
    udf.is_desktop_argument_profile_automation('Linux', '151.0', NULL, DATE '2026-09-06')
  ),
  mozfun.assert.equals(FALSE, udf.is_desktop_argument_profile_automation(NULL, NULL, NULL, NULL));
