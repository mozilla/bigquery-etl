-- Temporary logic due to performance regression; see
-- https://bugzilla.mozilla.org/show_bug.cgi?id=1763233
-- TODO: Restore logic from global_outages_v1 once performance issues are resolved.
SELECT
  *
FROM
  global_outages_staging_v1
