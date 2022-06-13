CREATE OR REPLACE FUNCTION vpn.channel_group(
  utm_campaign STRING,
  utm_content STRING,
  utm_medium STRING,
  utm_source STRING
)
RETURNS STRING AS (
  CASE
  WHEN
    (
      (
        utm_medium = "referral"
        AND utm_source = "www.mozilla.org-vpn-product-page"
        AND utm_campaign = "vpn-product-page"
      )
      OR (utm_medium = "organic" AND utm_source = "google-play")
    )
  THEN
    "Direct"
  WHEN
    (
      utm_medium LIKE "firefox%"
      OR utm_medium LIKE "android%"
      OR utm_medium IN ("ios", "monitor", "secure-proxy")
      OR (utm_medium = "tbwnp" AND utm_source = "thunderbird")
      OR utm_medium LIKE "pkt%"
      OR utm_source LIKE "%firefox%"
      OR utm_source LIKE "%fx-%"
      OR utm_source IN (
        "developer.mozilla.org",
        "addons.mozilla.org",
        "activity-stream",
        "accounts.firefox.com.cn",
        "accounts.firefox.com",
        "about-home"
      )
    )
  THEN
    "Product Owned"
  WHEN
    (utm_medium LIKE "paid%" OR utm_medium IN ("cpc", "display") OR utm_content LIKE "A144%")
  THEN
    "Marketing Paid"
  WHEN
    (
      utm_medium = "referral"
      OR utm_medium IN (
        "email",
        "mozilla-websites",
        "partnership",
        "snippet",
        "social",
        "organicsocial",
        "mozillaVPN",
        "featuredpages"
      )
      OR utm_source IN ("getpocket.com", "blog.mozilla.org", "www.mozilla.org")
      OR utm_source LIKE "mozilla.org-whatsnew%"
      OR utm_source LIKE "mozilla.org-welcome%"
      OR utm_campaign LIKE "%whatsnew%"
      OR utm_campaign LIKE "%welcome%"
    )
  THEN
    "Marketing Owned"
  WHEN
    vpn.normalize_utm_parameters(utm_campaign, utm_content, utm_medium, utm_source) = (
      "(not set)",
      "(not set)",
      "(none)",
      "(direct)"
    )
    OR utm_medium = "unknown"
  THEN
    "Unattributed"
  ELSE
    "Miscellaneous"
  END
);

-- Tests
SELECT
  assert.equals(
    channel_group,
    vpn.channel_group(utm_campaign, utm_content, utm_medium, utm_source)
  ),
FROM
  UNNEST(
    [
      STRUCT(
        -- expect
        "Miscellaneous" AS channel_group,
        -- inputs
        "campaign" AS utm_campaign,
        "content" AS utm_content,
        "medium" AS utm_medium,
        "source" AS utm_source
      ),
      STRUCT(
        -- expect
        "Unattributed" AS channel_group,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      )
    ]
  )
