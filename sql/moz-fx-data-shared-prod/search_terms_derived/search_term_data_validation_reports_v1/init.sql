CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.search_terms_derived.search_term_data_validation_reports_v1`(
    from_sanitization_job_finished_at timestamp,
    started_at timestamp,
    range_alarm boolean,
    range_low float,
    range_high float,
    num_ranges_compared timestamp,
    num_ranges_compared integer,
    range_test_vals string,
    mean_alarm boolean,
    mean_low float,
    mean_high float,
    num_moving_averages_compared integer,
    mean_test_vals string,
    metric string,
    full_lookback_window_num_days integer,
    test_window_num_days integer,
    moving_average_window_num_days integer,
    range_percentile_lower_bound float,
    range_percentile_upper_bound float,
    mean_percentile_lower_bound float,
    mean_percentile_upper_bound float
  );
