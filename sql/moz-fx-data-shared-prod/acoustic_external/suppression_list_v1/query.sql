WITH base_suppression_lists AS (
  SELECT
    email
  FROM
    `moz-fx-data-bq-fivetran.acoustic_sftp.suppression_export_v_1`
  UNION ALL
  SELECT
    email
  -- historic upload
  FROM
    `moz-fx-data-shared-prod.acoustic_external.2024-03-20_main_suppression_list`
),
suppression_list AS (
  SELECT DISTINCT
    email
  FROM
    base_suppression_lists
),
contacts AS (
  SELECT
    email,
    email_id
  FROM
    `moz-fx-data-marketing-prod.acoustic.contact_raw_v1`
)
SELECT
  email_id AS external_id,
  email,
  "unsubscribed" AS email_subscribe
FROM
  suppression_list
LEFT JOIN
  contacts
  USING (email)
GROUP BY
  ALL
