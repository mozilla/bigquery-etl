WITH daily_aggregates AS (
  SELECT
    report_date,
    root_domain,
    daily_domain_reports,
    SUM(daily_domain_reports) OVER (
      PARTITION BY
        root_domain
      ORDER BY
        report_date
      ROWS BETWEEN
        6 PRECEDING
        AND CURRENT ROW
    ) AS weekly_trend, -- Weekly trend for comparison
    COUNT(1) OVER (
      PARTITION BY
        root_domain
      ORDER BY
        report_date
      ROWS BETWEEN
        6 PRECEDING
        AND CURRENT ROW
    ) AS weekly_recurrence_count -- Count of weekly recurrence
  FROM
    `moz-fx-data-shared-prod.org_mozilla_broken_site_report_derived.broken_site_root_domain_daily_aggregates_v1`
  WHERE
    report_date
    BETWEEN @submission_date - 6
    AND @submission_date
)
SELECT
  *
FROM
  daily_aggregates
WHERE
  report_date = @submission_date
