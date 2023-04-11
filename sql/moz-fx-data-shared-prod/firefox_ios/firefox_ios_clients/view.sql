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
          OR adjust_info.adjust_network = ''
          THEN 'Unknown'
        WHEN adjust_info.adjust_network NOT IN (
            'Organic',
            'Google Organic Search',
            'Untrusted Devices',
            'Product Marketing (Owned media)',
            'Google Ads ACI'
          )
          THEN 'Other'
        ELSE adjust_info.adjust_network
      END AS adjust_network,
      adjust_info.adjust_ad_group,
      adjust_info.adjust_campaign,
      adjust_info.adjust_creative,
      adjust_info.submission_timestamp
    ) AS adjust_info,
    CASE
      WHEN install_source IS NULL
        OR install_source = ''
        THEN 'Unknown'
      WHEN install_source NOT IN (
          'com.android.vending'
        )  -- TODO: this needs to be changed
        THEN 'Other'
      ELSE install_source
    END AS install_source
  ),
FROM
  tmp.kik_firefox_ios_attribution_1st_version
