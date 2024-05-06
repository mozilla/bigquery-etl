WITH suppressions AS (
  SELECT
    LOWER(email) AS email
  FROM
    `moz-fx-data-shared-prod.marketing_suppression_list_derived.main_suppression_list_v1`
  UNION DISTINCT
  SELECT
    LOWER(primary_email) AS email
  FROM
    `moz-fx-data-shared-prod.ctms_braze.ctms_emails`
  WHERE
    has_opted_out_of_email = TRUE
)
SELECT
  *
FROM
  suppressions
