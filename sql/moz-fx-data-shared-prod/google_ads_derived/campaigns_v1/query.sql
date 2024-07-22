WITH campaign_names AS (
  SELECT
    customer_id,
    id AS campaign_id,
    name AS campaign_name,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1 AS is_most_recent_record,
  FROM
    `moz-fx-data-bq-fivetran`.ads_google_mmc.campaign_history
  QUALIFY
    is_most_recent_record
)
SELECT
  accounts.account_name,
  accounts.account_id,
  campaign_names.campaign_id,
  campaign_names.campaign_name,
  IF(
    -- Mozilla Firefox UAC account extracts some info from campaign names
    account_id = 2474006105,
    mozfun.google_ads.extract_segments_from_campaign_name(campaign_name),
    STRUCT(
      CAST(NULL AS STRING) AS campaign_region,
      CAST(NULL AS STRING) AS campaign_country_code,
      CAST(NULL AS STRING) AS campaign_language
    )
  ).*
FROM
  campaign_names
JOIN
  `moz-fx-data-shared-prod.google_ads_derived.accounts_v1` AS accounts
  ON accounts.account_id = campaign_names.customer_id
