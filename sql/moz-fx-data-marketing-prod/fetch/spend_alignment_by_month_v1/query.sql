WITH fetch_summary AS (
  SELECT
    month,
    year,
    ROUND(SUM(vendorNetSpend)) AS fetch_summary_spend,
  FROM
    `moz-fx-data-marketing-prod.fetch.summary_*`
  WHERE
    _TABLE_SUFFIX = (SELECT MAX(_table_suffix) FROM `moz-fx-data-marketing-prod.fetch.summary_*`)
  GROUP BY
    month,
    year
),
detailed_summary AS (
  SELECT
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    ROUND(SUM(vendorNetSpend)) AS detail_vendor_net_spend,
  FROM
    `moz-fx-data-marketing-prod.fetch.fetch_deduped`
  GROUP BY
    month,
    year
)
SELECT
  *,
  fetch_summary_spend - detail_vendor_net_spend AS variance,
FROM
  fetch_summary
LEFT JOIN
  detailed_summary
  USING (month, year)
ORDER BY
  year,
  month
