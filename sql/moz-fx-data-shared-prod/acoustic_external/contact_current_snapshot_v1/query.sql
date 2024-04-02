SELECT
  * EXCEPT (row_id)
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY email_id ORDER BY last_modified_date) AS row_id
    FROM
      `moz-fx-data-marketing-prod.acoustic.contact_v1`
  )
WHERE
  row_id = 1
