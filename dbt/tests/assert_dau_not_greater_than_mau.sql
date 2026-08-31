-- Singular (custom) test: DAU can never exceed MAU for a given row.
-- A dbt test passes when the query returns zero rows, so this selects violations.
SELECT
  submission_date,
  country,
  channel,
  dau,
  mau
FROM
  {{ ref('baseline_active_users_by_country_v1') }}
WHERE
  dau > mau
