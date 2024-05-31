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
)
SELECT
  *
FROM
  suppression_lists
-- remove older entries after the suppression list prune in April 2024
-- https://mozilla-hub.atlassian.net/browse/CTMS-163
WHERE
  suppressed_timestamp > "2024-03-31"
