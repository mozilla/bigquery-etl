{{ header }}

SELECT
    cd.*,
    cfs.first_seen_date,
    cd.submission_date = cfs.submission_date as is_new_profile
FROM
  `{{ daily_table }}` cd
LEFT JOIN
  `{{ first_seen_table }}` cfs
USING
  (client_id)
WHERE
  cd.submission_date > "2010-01-01"