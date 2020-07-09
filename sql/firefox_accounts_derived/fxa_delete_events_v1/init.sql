CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod`.firefox_accounts_derived.fxa_delete_events_v1
PARTITION BY
  DATE(submission_timestamp)
AS
WITH columns AS (
  SELECT
    CAST(NULL AS TIMESTAMP) AS submission_timestamp,
    CAST(NULL AS STRING) AS user_id,
    CAST(NULL AS STRING) AS hmac_user_id,
)
SELECT
  *
FROM
  columns
WHERE
  FALSE
