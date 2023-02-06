CREATE OR REPLACE FUNCTION norm.vpn_attribution(
  utm_campaign STRING,
  utm_content STRING,
  utm_medium STRING,
  utm_source STRING
)
RETURNS STRUCT<
  normalized_acquisition_channel STRING,
  normalized_campaign STRING,
  normalized_content STRING,
  normalized_medium STRING,
  normalized_source STRING,
  website_channel_group STRING
> AS (
  STRUCT(
      -- normalized_acquisition_channel
    CASE
      WHEN utm_source = "firefox-browser"
        THEN "Website: Firefox Browser"
      WHEN utm_campaign != "(not set)"
        OR utm_content != "(not set)"
        OR utm_medium != "(none)"
        OR utm_source != "(direct)"
        THEN "Website"
      ELSE "Unknown"
    END AS normalized_acquisition_channel,
    COALESCE(utm_campaign, "(not set)") AS normalized_campaign,
    COALESCE(utm_content, "(not set)") AS normalized_content,
    COALESCE(utm_medium, "(none)") AS normalized_medium,
    COALESCE(utm_source, "(direct)") AS normalized_source,
      -- website_channel_group
    CASE
      WHEN utm_source = "firefox-browser"
        THEN "Owned In-Product Channels"
      WHEN utm_medium IN ("banner", "cpc", "display", "paidsearch", "ppc", "affiliate", "cpm")
        THEN "Paid Channels"
      WHEN utm_medium IN ("email", "snippet")
        OR utm_source IN (
          "leanplum-push-notification",
          "www.mozilla.org-whatsnew",
          "www.mozilla.org-welcome"
        )
        OR utm_source LIKE "mozilla.org-whatsnew%"
        THEN "Marketing Owned Media Channels"
      ELSE "Unpaid Channels"
    END AS website_channel_group
  )
);

-- Tests
SELECT
  assert.json_equals(
    STRUCT(
      normalized_acquisition_channel,
      normalized_campaign,
      normalized_content,
      normalized_medium,
      normalized_source,
      website_channel_group
    ),
    norm.vpn_attribution(utm_campaign, utm_content, utm_medium, utm_source)
  ),
FROM
  UNNEST(
    [
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "campaign" AS normalized_campaign,
        "content" AS normalized_content,
        "medium" AS normalized_medium,
        "source" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        "campaign" AS utm_campaign,
        "content" AS utm_content,
        "medium" AS utm_medium,
        "source" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website: Firefox Browser" AS normalized_acquisition_channel,
        "campaign" AS normalized_campaign,
        "content" AS normalized_content,
        "medium" AS normalized_medium,
        "firefox-browser" AS normalized_source,
        "Owned In-Product Channels" AS website_channel_group,
        -- inputs
        "campaign" AS utm_campaign,
        "content" AS utm_content,
        "medium" AS utm_medium,
        "firefox-browser" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website: Firefox Browser" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "(none)" AS normalized_medium,
        "firefox-browser" AS normalized_source,
        "Owned In-Product Channels" AS website_channel_group,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        "firefox-browser" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "referral" AS normalized_medium,
        "www.mozilla.org-vpn-product-page" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        "referral" AS utm_medium,
        "www.mozilla.org-vpn-product-page" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "whatsnew85" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "(none)" AS normalized_medium,
        "mozilla.org-whatsnew85" AS normalized_source,
        "Marketing Owned Media Channels" AS website_channel_group,
        -- inputs
        "whatsnew85" AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        "mozilla.org-whatsnew85" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "welcome10" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "(none)" AS normalized_medium,
        "www.mozilla.org-welcome" AS normalized_source,
        "Marketing Owned Media Channels" AS website_channel_group,
        -- inputs
        "welcome10" AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        "www.mozilla.org-welcome" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "(none)" AS normalized_medium,
        "leanplum-push-notification" AS normalized_source,
        "Marketing Owned Media Channels" AS website_channel_group,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        "leanplum-push-notification" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "banner" AS normalized_medium,
        "(direct)" AS normalized_source,
        "Paid Channels" AS website_channel_group,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        "banner" AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "email" AS normalized_medium,
        "(direct)" AS normalized_source,
        "Marketing Owned Media Channels" AS website_channel_group,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        "email" AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Unknown" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "(none)" AS normalized_medium,
        "(direct)" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      )
    ]
  )
