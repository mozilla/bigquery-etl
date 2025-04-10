SELECT
  @as_of_date AS as_of_date,
  SAFE.JSON_VALUE(lookupData, '$.countryCode') AS country_code,
  COUNT(uid) AS total_rows
FROM
  `moz-fx-data-shared-prod.accounts_db_external.fxa_recovery_phones_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
    @as_of_date + 1,
    'UTC'
  )
GROUP BY
  country_code
