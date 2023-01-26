WITH campaign_names AS (
  SELECT
    id AS campaign_id,
    name AS campaign_name,
    row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1 AS is_most_recent_record,
  FROM
    `moz-fx-data-bq-fivetran`.google_ads.campaign_history
)
SELECT
  *
FROM
  campaign_names
WHERE
  is_most_recent_record
