-- Grain-uniqueness test for {{ ref('top_countries_by_active_users_v1') }}: returns any grain appearing more than
-- once (empty result = pass).
-- TODO: fill in the grain columns below (candidates from the model SELECT):
--   submission_date,
--   country,
--   channel,
--   daily_users,
SELECT
  -- TODO: grain columns,
  COUNT(*) AS row_count
FROM
  {{ ref('top_countries_by_active_users_v1') }}
GROUP BY
  -- TODO: grain columns
HAVING
  COUNT(*) > 1
