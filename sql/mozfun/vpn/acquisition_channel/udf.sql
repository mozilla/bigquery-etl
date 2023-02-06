CREATE OR REPLACE FUNCTION vpn.acquisition_channel(
  utm_campaign STRING,
  utm_content STRING,
  utm_medium STRING,
  utm_source STRING
)
RETURNS STRING AS (
  CASE
    WHEN utm_source = "firefox-browser"
      THEN "Website: Firefox Browser"
    WHEN vpn.normalize_utm_parameters(utm_campaign, utm_content, utm_medium, utm_source) = (
        "(not set)",
        "(not set)",
        "(none)",
        "(direct)"
      )
      THEN "Unknown"
    ELSE "Website"
  END
);

-- Tests
SELECT
  assert.equals(
    acquisition_channel,
    vpn.acquisition_channel(utm_campaign, utm_content, utm_medium, utm_source)
  ),
FROM
  UNNEST(
    [
      STRUCT(
        -- expect
        "Website" AS acquisition_channel,
        -- inputs
        "campaign" AS utm_campaign,
        "content" AS utm_content,
        "medium" AS utm_medium,
        "source" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website: Firefox Browser" AS acquisition_channel,
        -- inputs
        "campaign" AS utm_campaign,
        "content" AS utm_content,
        "medium" AS utm_medium,
        "firefox-browser" AS utm_source
      ),
      STRUCT(
        -- expect
        "Website: Firefox Browser" AS acquisition_channel,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        "firefox-browser" AS utm_source
      ),
      STRUCT(
        -- expect
        "Unknown" AS acquisition_channel,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      ),
      STRUCT(
        -- expect
        "Unknown" AS acquisition_channel,
        -- inputs
        "(not set)" AS utm_campaign,
        "(not set)" AS utm_content,
        "(none)" AS utm_medium,
        "(direct)" AS utm_source
      )
    ]
  )
