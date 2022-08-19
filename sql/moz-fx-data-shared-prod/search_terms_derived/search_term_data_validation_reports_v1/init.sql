CREATE TABLE IF NOT EXISTS
  `mozdata.search_terms_unsanitized_analysis.prototype_data_validation_reports_v1`(
    from_sanitization_job_finished_at timestamp,
    started_at timestamp,
    range_alarm boolean,
    range_low float64,
    range_high float64,
    num_ranges_compared integer,
    range_test_vals string,
    mean_alarm boolean,
    mean_low float64,
    mean_high float64,
    num_moving_averages_compared integer,
    mean_test_vals string,
    metric string,
    full_lookback_window_num_days integer,
    test_window_num_days integer,
    moving_average_window_num_days integer,
    range_percentile_lower_bound float64,
    range_percentile_upper_bound float64,
    mean_percentile_lower_bound float64,
    mean_percentile_upper_bound float64
  );
