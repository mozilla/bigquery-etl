CREATE OR REPLACE VIEW
  firefox_ios.firefox_ios_clients
AS
SELECT
  * REPLACE (
    COALESCE(
      first_seen_date,
      metadata.min_first_session_ping_submission_date,
      metadata.min_metrics_ping_submission_date
    ) AS first_seen_date,
    STRUCT(
      CASE
        WHEN adjust_info.adjust_network IS NULL
          THEN 'Unknown'
        WHEN adjust_info.adjust_network NOT IN (
            'Apple Search Ads',
            'Product Marketing (Owned media)',
            'product-owned'
          )
          THEN 'Other'
        ELSE adjust_info.adjust_network
      END AS adjust_network,
      adjust_info.adjust_ad_group,
      adjust_info.adjust_campaign,
      adjust_info.adjust_creative,
      adjust_info.submission_timestamp
    ) AS adjust_info
  ),
FROM
  firefox_ios_derived.firefox_ios_clients_v1
