-- Query generated via `mobile_kpi_support_metrics` SQL generator.
# warn
{{ min_row_count(1), "WHERE metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)" }}