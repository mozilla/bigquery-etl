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
    STRUCT(
      mozfun.google_ads.extract_region_from_campaign_name(campaign_name) AS campaign_region,
      mozfun.google_ads.extract_country_code_from_campaign_name(campaign_name) AS campaign_country_code,
      mozfun.google_ads.extract_language_from_campaign_name(campaign_name) AS campaign_language
    ),
    STRUCT(
      CAST(NULL AS STRING) AS campaign_region,
      CAST(NULL AS STRING) AS campaign_country_code,
      CAST(NULL AS STRING) AS campaign_language
    )
  ).*
FROM
  campaign_names
JOIN
  google_ads_derived.accounts_v1 AS accounts
  ON accounts.account_id = campaign_names.customer_id
