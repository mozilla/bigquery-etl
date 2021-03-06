CREATE OR REPLACE FUNCTION norm.vpn_attribution(
  provider STRING,
  referrer STRING,
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
  (
    WITH stage_1 AS (
      SELECT
        REGEXP_EXTRACT(
          referrer,
          "https://((?:fpn|accounts)[.]firefox[.]com)/"
        ) AS internal_referrer,
        REGEXP_EXTRACT(
          -- extract search engine from referrer
          REGEXP_EXTRACT(
            referrer,
            -- use same pattern as normalized_medium = "organic"
            "www[.]google|duckduckgo|bing|search[.]yahoo|ecosia[.]org|yandex|qwant[.]com|baidu|ask|naver|sogou"
          ),
          -- strip prefix from google and yahoo patterns
          "(?:www[.]|search[.])?(.*)"
        ) AS organic_referrer,
    ),
    stage_2 AS (
      SELECT
        *,
        -- normalized_medium
        CASE
        WHEN
          utm_medium IS NOT NULL
          OR utm_source IS NOT NULL
          OR utm_content IS NOT NULL
          OR utm_campaign IS NOT NULL
        THEN
          utm_medium
        WHEN
          organic_referrer IS NOT NULL
        THEN
          "organic"
        WHEN
          internal_referrer IS NOT NULL
        THEN
          "referral"
        WHEN
          referrer IS NULL
        THEN
          "(none)"
        END
        AS normalized_medium,
      FROM
        stage_1
    ),
    stage_3 AS (
      SELECT
        -- normalized_acquisition_channel
        CASE
        WHEN
          utm_source = "firefox-browser"
        THEN
          "Website: Firefox Browser"
        WHEN
          utm_medium = '["referral","referral"]'
          -- preserve legacy behavior
          AND (referrer LIKE "%app-store%") IS NOT TRUE
        THEN
          "Possible Attribution Error - Monitor"
        WHEN
          referrer LIKE "%app-store%"
          AND utm_medium IS NULL
          AND utm_source IS NULL
          AND utm_campaign IS NULL
          AND utm_content IS NULL
        THEN
          CONCAT("App Store: ", SPLIT(referrer, "-")[offset(2)])
        WHEN
          provider = "Apple Store IAP"
        THEN
          provider
        WHEN
          normalized_medium IS NOT NULL
          OR utm_source IS NOT NULL
          OR utm_campaign IS NOT NULL
          OR utm_content IS NOT NULL
        THEN
          "Website"
        ELSE
          "Unknown"
        END
        AS normalized_acquisition_channel,
        -- normalized_campaign
        IF(
          utm_campaign IS NULL
          AND (
            (utm_content IS NULL AND utm_medium IS NULL AND utm_source IS NULL AND referrer IS NULL)
            OR utm_source = "firefox-browser"
            OR NULLIF(normalized_medium, '["referral","referral"]') IS NOT NULL
          ),
          "(not set)",
          utm_campaign
        ) AS normalized_campaign,
        -- normalized_content
        IF(
          utm_content IS NULL
          AND (
            (
              utm_campaign IS NULL
              AND utm_medium IS NULL
              AND utm_source IS NULL
              AND referrer IS NULL
            )
            OR utm_source = "firefox-browser"
            OR NULLIF(normalized_medium, '["referral","referral"]') IS NOT NULL
          ),
          "(not set)",
          utm_content
        ) AS normalized_content,
        --
        normalized_medium,
        -- normalized_source
        CASE
        WHEN
          utm_medium IS NULL
          AND utm_source IS NULL
          AND utm_content IS NULL
          AND utm_campaign IS NULL
        THEN
          CASE
          WHEN
            organic_referrer IS NOT NULL
          THEN
            organic_referrer
          WHEN
            internal_referrer IS NOT NULL
          THEN
            internal_referrer
          WHEN
            referrer IS NULL
          THEN
            "(direct)"
          END
        WHEN
          utm_medium = "referral"
          AND (
            utm_source IS NULL
            AND referrer LIKE "%www.mozilla.org%"
            OR utm_source = "www.mozilla.org-vpn-product-page"
          )
        THEN
          "www.mozilla.org"
        ELSE
          utm_source
        END
        AS normalized_source,
      FROM
        stage_2
    )
    SELECT AS STRUCT
      *,
      -- website_channel_group
      CASE
      WHEN
        normalized_medium IN ("banner", "cpc", "display", "paidsearch", "ppc", "affiliate", "cpm")
        AND normalized_acquisition_channel = "Website"
      THEN
        "Paid Channels"
      WHEN
        normalized_medium IN ("email", "snippet")
        OR utm_source IN ("leanplum-push-notification")
        OR utm_source LIKE "%whatsnew%"
      THEN
        "Marketing Owned Media Channels"
      WHEN
        utm_source = "firefox-browser"
      THEN
        "Owned In-Product Channels"
      ELSE
        "Unpaid Channels"
      END
      AS website_channel_group,
    FROM
      stage_3
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
    norm.vpn_attribution(provider, referrer, utm_campaign, utm_content, utm_medium, utm_source)
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
        "provider" AS provider,
        "referrer" AS referrer,
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
        "provider" AS provider,
        "referrer" AS referrer,
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
        NULL AS normalized_medium,
        "firefox-browser" AS normalized_source,
        "Owned In-Product Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
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
        "www.mozilla.org" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        "referral" AS utm_medium,
        "www.mozilla.org-vpn-product-page" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "referral" AS normalized_medium,
        "www.mozilla.org" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        "www.mozilla.org" AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        "referral" AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        NULL AS normalized_campaign,
        NULL AS normalized_content,
        NULL AS normalized_medium,
        "whatsnew" AS normalized_source,
        "Marketing Owned Media Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        "whatsnew" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        NULL AS normalized_campaign,
        NULL AS normalized_content,
        NULL AS normalized_medium,
        "leanplum-push-notification" AS normalized_source,
        "Marketing Owned Media Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
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
        NULL AS normalized_source,
        "Paid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
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
        NULL AS normalized_source,
        "Marketing Owned Media Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        "email" AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Possible Attribution Error - Monitor" AS normalized_acquisition_channel,
        NULL AS normalized_campaign,
        NULL AS normalized_content,
        '["referral","referral"]' AS normalized_medium,
        NULL AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        '["referral","referral"]' AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Unknown" AS normalized_acquisition_channel,
        NULL AS normalized_campaign,
        NULL AS normalized_content,
        NULL AS normalized_medium,
        NULL AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        "referrer" AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "organic" AS normalized_medium,
        "google" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        "www.google.com" AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "organic" AS normalized_medium,
        "duckduckgo" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        "FXA" AS provider,
        "https://duckduckgo.com/" AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "referral" AS normalized_medium,
        "fpn.firefox.com" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        "https://fpn.firefox.com/" AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "App Store: mozilla vpn" AS normalized_acquisition_channel,
        NULL AS normalized_campaign,
        NULL AS normalized_content,
        NULL AS normalized_medium,
        NULL AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        "app-store-mozilla vpn-1.2" AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Apple Store IAP" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "(none)" AS normalized_medium,
        "(direct)" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        "Apple Store IAP" AS provider,
        NULL AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Website" AS normalized_acquisition_channel,
        "(not set)" AS normalized_campaign,
        "(not set)" AS normalized_content,
        "(none)" AS normalized_medium,
        "(direct)" AS normalized_source,
        "Unpaid Channels" AS website_channel_group,
        -- inputs
        NULL AS provider,
        NULL AS referrer,
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      )
    ]
  )
