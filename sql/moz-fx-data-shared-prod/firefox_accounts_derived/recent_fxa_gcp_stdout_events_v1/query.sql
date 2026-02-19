SELECT
  * EXCEPT (partition_date)
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_gcp_stdout_events_v1_live`
WHERE
  partition_date >= CURRENT_DATE() - 2
  AND DATE(`timestamp`) >= CURRENT_DATE() - 1
