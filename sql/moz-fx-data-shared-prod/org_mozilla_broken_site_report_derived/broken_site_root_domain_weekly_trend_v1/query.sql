with daily_aggregates as (
  SELECT
    report_date,
    root_domain,
    daily_domain_reports,
    SUM(daily_domain_reports) OVER (PARTITION BY root_domain ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_trend, -- Weekly trend for comparison
    count(1) OVER (PARTITION BY root_domain ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_recurrence_count -- Count of weekly recurrence
  FROM
    `moz-fx-data-shared-prod.org_mozilla_broken_site_report_derived.broken_site_root_domain_daily_aggregates_v1`
  where
    report_date between @submission_date - 6 and @submission_date
)

select
  *
from daily_aggregates
where
  report_date = @submission_date
