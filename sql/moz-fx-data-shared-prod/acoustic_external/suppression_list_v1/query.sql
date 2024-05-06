WITH suppression_lists AS (
  SELECT
    email,
    CASE
      WHEN REGEXP_CONTAINS(opt_in_date, r"\d{2}/\d{2}/\d{4}")
        THEN PARSE_TIMESTAMP("%m/%d/%Y %I:%M %p", REPLACE(opt_in_date, "\"", ""))
      WHEN REGEXP_CONTAINS(opt_in_date, r"\d{4}[-/]\d{2}[-/]\d{2}")
        THEN PARSE_TIMESTAMP("%Y-%m-%d %I:%M %p", REPLACE(REPLACE(opt_in_date, "\"", ""), "/", "-"))
    END AS suppressed_timestamp,
    opt_in_details AS suppression_reason,
  FROM
    `moz-fx-data-bq-fivetran.acoustic_sftp.suppression_export_v_1`
  UNION ALL
  SELECT
    email,
    PARSE_TIMESTAMP("%Y-%m-%d %I:%M %p", opt_in_date) AS suppressed_timestamp,
    opt_in_details AS suppression_reason
  -- historic upload
  FROM
    `moz-fx-data-shared-prod.acoustic_external.2024-03-20_main_suppression_list`
)
SELECT
  *
FROM
  suppression_lists
