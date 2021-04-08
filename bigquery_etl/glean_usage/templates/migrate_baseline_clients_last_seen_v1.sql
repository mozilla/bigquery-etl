{{ header }}

SELECT
    cls.*,
    cfs.first_seen_date,
    cls.submission_date = cfs.submission_date as is_new_profile
FROM
  `{{ last_seen_table }}` cls
LEFT JOIN
  `{{ first_seen_table }}` cfs
USING
  (client_id)
WHERE
  cls.submission_date > "2010-01-01"
