SELECT
  * EXCEPT (partition_date)
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_gcp_stderr_events_v1_live`
WHERE
  (
    partition_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  )
  AND DATE(`timestamp`) = @submission_date
