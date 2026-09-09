SELECT
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.nimbus_recorded_targeting_context_v1`
WHERE
  submission_date
  BETWEEN DATE_SUB(@submission_date, INTERVAL 6 DAY)
  AND @submission_date
  AND ABS(MOD(FARM_FINGERPRINT(client_id), 100)) < 10
  AND client_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date DESC) = 1
