CREATE TEMP FUNCTION yes_no_to_int(val STRING) AS (
  CASE
    WHEN LOWER(val) = "no"
      THEN 0
    WHEN LOWER(val) = "yes"
      THEN 1
    ELSE CAST(val AS INTEGER)
  END
);

SELECT
  * EXCEPT (_file, _line, _fivetran_synced, _modified, last_modified_date) REPLACE(
    yes_no_to_int(double_opt_in) AS double_opt_in,
    yes_no_to_int(fxa_account_deleted) AS fxa_account_deleted,
    yes_no_to_int(has_opted_out_of_email) AS has_opted_out_of_email
  ),
  DATE(@submission_date) AS last_modified_date,
FROM
  `moz-fx-data-bq-fivetran.acoustic_sftp.contact_export_v_1`
WHERE
  DATE(@submission_date) = mozfun.datetime_util.fxa_parse_date(last_modified_date)
