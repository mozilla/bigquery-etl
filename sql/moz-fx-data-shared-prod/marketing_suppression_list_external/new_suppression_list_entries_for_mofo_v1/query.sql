SELECT
  main.email
FROM
  `moz-fx-data-shared-prod.marketing_suppression_list_derived.main_suppression_list_v1` AS main
LEFT JOIN
  `moz-fx-data-shared-prod.marketing_suppression_list_external.campaign_monitor_suppression_list_v1` AS current_mofo
  ON main.email = current_mofo.email
WHERE
  current_mofo.email IS NULL
