WITH reports AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.text2.broken_site_report_description != "" -- Exclude empty descriptions
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        document_id
      ORDER BY
        submission_timestamp
    ) = 1 -- Only include the first submission for each document_id
)
SELECT
  DATE(submission_timestamp) AS report_date,
  REGEXP_EXTRACT(
    metrics.url2.broken_site_report_url,
    r'https?://(?:www\.)?([^/]+)'
  ) AS root_domain, -- Extract root domain
  COUNT(*) AS daily_domain_reports
FROM
  reports
GROUP BY
  report_date,
  root_domain
