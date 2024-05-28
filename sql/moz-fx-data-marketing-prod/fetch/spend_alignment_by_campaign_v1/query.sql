WITH fetch_summary AS (
  SELECT
    campaign,
    ROUND(SUM(vendorNetSpend)) AS fetch_summary_spend,
  FROM
    `moz-fx-data-marketing-prod.fetch.summary_*`
  WHERE
    _TABLE_SUFFIX = (SELECT MAX(_table_suffix) FROM `moz-fx-data-marketing-prod.fetch.summary_*`)
  GROUP BY
    campaign
),
detailed_summary AS (
  SELECT
    campaign,
    ROUND(SUM(vendorNetSpend)) AS detail_vendor_net_spend,
  FROM
    `moz-fx-data-marketing-prod.fetch.fetch_deduped`
  GROUP BY
    campaign
)
SELECT
  *,
  fetch_summary_spend - detail_vendor_net_spend AS variance,
FROM
  fetch_summary
LEFT JOIN
  detailed_summary
  USING (campaign)
ORDER BY
  fetch_summary_spend DESC
