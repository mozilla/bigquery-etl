CREATE OR REPLACE FUNCTION vpn.normalize_utm_parameters(
  utm_campaign STRING,
  utm_content STRING,
  utm_medium STRING,
  utm_source STRING
)
RETURNS STRUCT<utm_campaign STRING, utm_content STRING, utm_medium STRING, utm_source STRING> AS (
  STRUCT(
    IFNULL(utm_campaign, "(not set)") AS utm_campaign,
    IFNULL(utm_content, "(not set)") AS utm_content,
    IFNULL(utm_medium, "(none)") AS utm_medium,
    IFNULL(utm_source, "(direct)") AS utm_source
  )
);

-- Tests
SELECT
  assert.json_equals(
    expect,
    vpn.normalize_utm_parameters(utm_campaign, utm_content, utm_medium, utm_source)
  ),
FROM
  UNNEST(
    [
      STRUCT(
        -- expect
        STRUCT(
          "campaign" AS utm_campaign,
          "content" AS utm_content,
          "medium" AS utm_medium,
          "source" AS utm_source
        ) AS expect,
        -- inputs
        "campaign" AS utm_campaign,
        "content" AS utm_content,
        "medium" AS utm_medium,
        "source" AS utm_source
      ),
      STRUCT(
        -- expect
        STRUCT(
          "(not set)" AS utm_campaign,
          "(not set)" AS utm_content,
          "(none)" AS utm_medium,
          "(direct)" AS utm_source
        ) AS expect,
        -- inputs
        NULL AS utm_campaign,
        NULL AS utm_content,
        NULL AS utm_medium,
        NULL AS utm_source
      )
    ]
  )
