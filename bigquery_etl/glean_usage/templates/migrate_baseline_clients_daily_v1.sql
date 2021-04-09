{{ header }}

SELECT
    cd.*,
    cfs.first_seen_date,
    t.submission_date = cfs.first_seen as is_new_profile
FROM
  `{{ daily_table }}` AS t
LEFT JOIN
  `{{ first_seen_table }}` cfs
USING
  (client_id)
WHERE
  t.submission_date > "2010-01-01"