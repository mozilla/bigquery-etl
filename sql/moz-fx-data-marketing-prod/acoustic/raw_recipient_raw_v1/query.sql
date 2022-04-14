SELECT
  * EXCEPT (_file, _line, _fivetran_synced, _modified),
  DATE(@submission_date) AS submission_date,
FROM
  `moz-fx-data-bq-fivetran.acoustic_sftp.raw_recipient_export_v1`
