SELECT
  * EXCEPT (_file, _line, _fivetran_synced, _modified, last_modified_date),
  DATE(@submission_date) AS last_modified_date,
FROM
  `moz-fx-data-bq-fivetran.acoustic_sftp.contact_export_v_1`
WHERE
  DATE(@submission_date) = mozfun.datetime_util.fxa_parse_date(last_modified_date)
