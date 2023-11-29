WITH campaign_names AS (
  SELECT
    customer_id,
    id AS campaign_id,
    name AS campaign_name,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1 AS is_most_recent_record,
  FROM
    `moz-fx-data-bq-fivetran`.ads_google_mmc.campaign_history
)
SELECT
  accounts.account_name,
  accounts.account_id,
  campaign_names.campaign_id,
  campaign_names.campaign_name,
  campaign_names.is_most_recent_record
FROM
  campaign_names
JOIN
  `moz-fx-data-shared`.google_ads_derived.accounts_v1 AS accounts
ON
  accounts.account_id = campaign_names.customer_id
WHERE
  campaign_names.is_most_recent_record
