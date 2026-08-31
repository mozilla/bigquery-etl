-- Singular (custom) test: the by_country model has one row per
-- (submission_date, country, channel). Returns any grain that appears more than once.
-- Demonstrates a grain-uniqueness check without needing the dbt_utils package
SELECT
  submission_date,
  country,
  channel,
  COUNT(*) AS row_count
FROM
  {{ ref('baseline_active_users_by_country_v1') }}
GROUP BY
  submission_date,
  country,
  channel
HAVING
  COUNT(*) > 1
