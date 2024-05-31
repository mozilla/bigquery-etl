CREATE OR REPLACE FUNCTION serp_events.ad_blocker_inferred(num_loaded INT, num_blocked INT)
RETURNS BOOL AS (
  num_loaded > 0
  AND num_blocked = num_loaded
);

-- Tests
SELECT
  assert.equals(FALSE, serp_events.ad_blocker_inferred(10, 0)),
  assert.equals(TRUE, serp_events.ad_blocker_inferred(10, 10)),
  assert.equals(FALSE, serp_events.ad_blocker_inferred(0, 0))
