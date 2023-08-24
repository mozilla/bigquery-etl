from google.cloud import bigquery
from datetime import date, timedelta, datetime
from collections import namedtuple

import numpy as np
import pandas as pd
import asyncio
import re
import json
import string


def calculate_data_validation_metrics(metadata_source, languages_source):
    """
    Calculate metrics for determining whether our search volume is changing in ways that might invalidate our current sanitization model.

    Arguments:

    - metadata_source: a string. The name of the table containing the metadata to be fetched.
    - languages_source: a string. The name of the table containing language distributions for search term jobs.

    Returns: A dataframe of the data validation metrics for the sanitization jobs.
    """
    if re.fullmatch(r"[A-Za-z0-9\.\-\_]+", metadata_source):
        metadata_source_no_injection = metadata_source
    else:
        raise Exception(
            "metadata_source in incorrect format. This should be a fully qualified table name like myproject.mydataset.my_table"
        )

    if re.fullmatch(r"[A-Za-z0-9\.\-\_]+", languages_source):
        languages_source_no_injection = languages_source
    else:
        raise Exception(
            "metadata_source in incorrect format. This should be a fully qualified table name like myproject.mydataset.my_table"
        )

    # We are using f-strings here because BQ does not allow table names to be parametrized
    # and we need to be able to run the same script in the staging and prod db environments for reliable testing outcomes.
    SUCCESSFUL_SANITIZATION_JOB_RUN_METADATA = f"""
    SELECT
        finished_at,
        SAFE_DIVIDE(total_search_terms_removed_by_sanitization_job, total_search_terms_analyzed) AS pct_sanitized_search_terms,
        SAFE_DIVIDE(contained_at, total_search_terms_analyzed) AS pct_sanitized_contained_at,
        SAFE_DIVIDE(contained_numbers, total_search_terms_analyzed) AS pct_sanitized_contained_numbers,
        SAFE_DIVIDE(contained_name, total_search_terms_analyzed) AS pct_sanitized_contained_name,
        SAFE_DIVIDE(sum_terms_containing_us_census_surname, total_search_terms_analyzed) AS pct_terms_containing_us_census_surname,
        SAFE_DIVIDE(sum_uppercase_chars_all_search_terms, sum_chars_all_search_terms) AS pct_uppercase_chars_all_search_terms,
        SAFE_DIVIDE(sum_words_all_search_terms, total_search_terms_analyzed) AS avg_words_all_search_terms,
        1 - SAFE_DIVIDE(languages.english_count, languages.all_languages_count) AS pct_terms_non_english
        FROM `{metadata_source_no_injection}` AS metadata
    JOIN 
    (
        SELECT 
            job_start_time,
            max(case when language_code = 'en' then search_term_count end) english_count,
            sum(search_term_count) as all_languages_count,
        FROM `{languages_source_no_injection}` 
        GROUP BY job_start_time
    ) AS languages
    ON metadata.started_at = languages.job_start_time
    WHERE status = 'SUCCESS'
    ORDER BY finished_at ASC;
    """
    client = bigquery.Client()
    query_job = client.query(SUCCESSFUL_SANITIZATION_JOB_RUN_METADATA)
    results_as_dataframe = query_job.result().to_dataframe()

    return results_as_dataframe


def export_data_validation_metrics_to_bigquery(dataframe, destination_table_id):
    """
    Append data validation metrics to the BigQuery table tracking these metrics from job metadata.

    Arguments:
    - dataframe: A dataframe of validation metrics to be added.
    - destination_table_id: the fully qualified name of the table for the data to be exported into.

    Returns: Nothing.
    It does print a result value as a cursory logging mechanism. That result object can be parsed and logged to wherever we like.
    """
    client = bigquery.Client()

    schema = [
        bigquery.SchemaField("finished_at", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "pct_sanitized_search_terms", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "pct_sanitized_contained_at", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "pct_sanitized_contained_numbers", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "pct_sanitized_contained_name", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "pct_terms_containing_us_census_surname",
            bigquery.enums.SqlTypeNames.FLOAT64,
        ),
        bigquery.SchemaField(
            "pct_uppercase_chars_all_search_terms", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "avg_words_all_search_terms", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "pct_terms_non_english", bigquery.enums.SqlTypeNames.FLOAT64
        ),
    ]

    destination_table = bigquery.Table(destination_table_id)
    job = client.insert_rows_from_dataframe(
        table=destination_table, dataframe=dataframe, selected_fields=schema
    )

    print(job)


def retrieve_data_validation_metrics(metrics_source):
    """
    Pull all the sanitization job data validation metrics.

    Arguments:

    - metadata_source: a string. The name of the table containing the data validation metrics to be fetched.

    Returns: A dataframe of the data validation metrics.
    """
    if re.fullmatch(r"[A-Za-z0-9\.\-\_]+", metrics_source):
        metrics_source_no_injection = metrics_source
    else:
        raise Exception(
            "metadata_source in incorrect format. This should be a fully qualified table name like myproject.mydataset.my_table"
        )

    # We are using f-strings here because BQ does not allow table names to be parametrized
    # and we need to be able to run the same script in the staging and prod db environments for reliable testing outcomes.
    DATA_VALIDATION_METRICS_QUERY = f"""
    SELECT
        *
        FROM `{metrics_source_no_injection}` AS metadata
    ORDER BY finished_at ASC;
    """
    client = bigquery.Client()
    query_job = client.query(DATA_VALIDATION_METRICS_QUERY)
    results_as_dataframe = query_job.result().to_dataframe()

    return results_as_dataframe


def range_check(
    validation_data: pd.DataFrame,
    metric: str,
    full_lookback_window: int,
    test_window: int,
    range_lower_bound: float,
    range_upper_bound: float,
):
    print(f"Performing range check for metric: {metric}")
    """
    Determines if all the values in a test window of days fall inside some percentile of the normal range for a set of comparison values in a comparison window of days.

    Inputs:

    - validation_data: the dataframe with the data in it to be checked.
    ASSUMES the presence of a 'finished_at' column, whose date is used to calculate lookback and test windows.
    - metric: the name of the column in the input dataframe on which to perform the check.
    - full_lookback_window: an integer number of days that the comparison set should cover.
    - test_window. an integer number of days that the test set should cover.
    ASSUMES that the test window immediately succeeds the full_lookback_window.
    - range_lower_bound: a float between 0 and 1 indicating the lower edge of the window of normal values from the comparison set
    inside which at least one of the values in the test set should fall.
    - range_upper_bound: a float between 0 and 1 indicating the upper edge of the window of normal values from the comparison set
    inside which at least one of the values in the test set should fall.


    Outputs:
    - finished_at: the finished_at timestamp of the job run to which this check applies.
    - num_values_compared: an integer representing the total number of range values included in this comparison.
    - should_trigger: a bool indicating whether the values in the test window are all falling OUTSIDE the expected range.
    - range_lower: a float. The lower bound of the expected range calculated from comparison values.
    - range_upper: a float. The upper bound of the expected range calculated from comparison values.
    - test_range: a list. The entirety of the test values.

    """
    if not (0 < range_lower_bound < 1 and 0 < range_upper_bound < 1):
        raise Exception(
            "range_lower_bound and range_upper_bound should both be between zero (0) and one (1)."
        )

    if "finished_at" not in validation_data.columns.values:
        raise Exception("dataframe must include a finished_at column.")

    if metric not in validation_data.columns.values:
        raise Exception(f'dataframe does not include target metric "{metric}"')

    today = date.today()
    latest_finished_at = max(validation_data["finished_at"])

    test_earliest_date = today - timedelta(days=test_window)

    comparison_earliest_date = test_earliest_date - timedelta(days=full_lookback_window)

    comparison_values = validation_data["finished_at"].apply(
        lambda m: comparison_earliest_date < m.date() <= test_earliest_date
    )
    test_values = validation_data["finished_at"].apply(
        lambda m: test_earliest_date < m.date() <= today
    )

    comparison_range = validation_data.loc[comparison_values]
    test_range = validation_data.loc[test_values]

    range_lower, range_upper = comparison_range[metric].quantile(
        q=[range_lower_bound, range_upper_bound]
    )

    should_trigger = len(test_range[metric]) != 0 and (
        all(test_range[metric] > range_upper) or all(test_range[metric] < range_lower)
    )

    print(f"Completed range check for metric: {metric}")
    return (
        latest_finished_at,
        len(comparison_range),
        should_trigger,
        range_lower,
        range_upper,
        list(test_range[metric]),
    )


def mean_check(
    validation_data: pd.DataFrame,
    metric: str,
    full_lookback_window: int,
    test_window: int,
    moving_average_window: int,
    mean_lower_bound: float,
    mean_upper_bound: float,
):
    print(f"Performing mean check for metric: {metric}")

    """
    Determines if all the moving averages in a test window of days fall inside some percentile of the moving average for a set of comparison values in a comparison window of days.

    Inputs:

    - validation_data: the dataframe with the data in it to be checked.
    ASSUMES the presence of a 'finished_at' column, whose date is used to calculate lookback and test windows.
    - metric: the name of the column in the input dataframe on which to perform the check.
    - full_lookback_window: an integer number of days that the comparison set should cover.
    - test_window. an integer number of days that the test set should cover.
    ASSUMES that the test window immediately succeeds the full_lookback_window.
    - moving_average_window: an integer. Number of prior days over which to calculate an average for a given day.
    - mean lower bound: a float between 0 and 1 indicating the lower edge of the window of normal values from the comparison set
    inside which at least one of the values in the test set should fall.
    - mean upper bound: a float between 0 and 1 indicating the upper edge of the window of normal values from the comparison set
    inside which at least one of the values in the test set should fall.


    Outputs:
    - finished_at: the finished_at timestamp of the job run to which this check applies.
    - num_moving_averages_compared: an integer representing the total number of moving average values included in this comparison.
    - should_trigger: a bool indicating whether the values in the test window are all falling OUTSIDE the expected range.
    - mean_lower: a float. The lower bound of the expected range of moving averages calculated from comparison values.
    - mean_upper: a float. The upper bound of the expected range of moving averages calculated from comparison values.
    - moving_average_windo: an integer. The moving average window passed into the function.
    - test_moving_averages: a list. The entirety of the test values.

    """
    if not (0 < mean_lower_bound < 1 and 0 < mean_upper_bound < 1):
        raise Exception(
            "mean_lower_bound and mean_upper_bound should both be between zero (0) and one (1)."
        )

    if "finished_at" not in validation_data.columns.values:
        raise Exception("dataframe must include a finished_at column.")

    if metric not in validation_data.columns.values:
        raise Exception(f'dataframe does not include target metric "{metric}"')

    today = date.today()
    latest_finished_at = max(validation_data["finished_at"])

    test_earliest_date = today - timedelta(days=test_window)
    comparison_earliest_date = test_earliest_date - timedelta(days=full_lookback_window)

    x_day_moving_average = f"{moving_average_window}_day_{metric}_moving_avg"
    validation_data[x_day_moving_average] = (
        validation_data[metric]
        .rolling(window=moving_average_window, min_periods=0)
        .mean()
    )

    comparison_values = validation_data["finished_at"].apply(
        lambda m: comparison_earliest_date < m.date() <= test_earliest_date
    )
    test_values = validation_data["finished_at"].apply(
        lambda m: test_earliest_date < m.date() <= today
    )

    comparison_range = validation_data.loc[comparison_values]
    test_range = validation_data.loc[test_values]

    mean_lower, mean_upper = comparison_range[x_day_moving_average].quantile(
        q=[mean_lower_bound, mean_upper_bound]
    )

    test_moving_averages = test_range[x_day_moving_average]
    should_trigger = len(test_moving_averages) != 0 and (
        all(test_moving_averages > mean_upper) or all(test_moving_averages < mean_lower)
    )
    num_moving_averages_compared = int(
        comparison_range[x_day_moving_average].notna().sum()
    )

    print(f"Completed mean check for metric: {metric}")
    return (
        latest_finished_at,
        num_moving_averages_compared,
        should_trigger,
        mean_lower,
        mean_upper,
        moving_average_window,
        list(test_moving_averages),
    )


def record_validation_results(val_df, destination_table):
    print(f"Recording validation results to destination table: {destination_table}")

    InputSet = namedtuple(
        "InputSet",
        "name full_lookback_window range_test_window range_lower_bound range_upper_bound mean_test_window mean_lower_bound mean_upper_bound moving_average_window",
    )
    client = bigquery.Client()
    started_at = datetime.utcnow()

    for metric in [
        InputSet(
            name="pct_sanitized_search_terms",
            full_lookback_window=90,
            range_test_window=4,
            range_lower_bound=0.125,
            range_upper_bound=0.875,
            mean_test_window=8,
            mean_lower_bound=0.01,
            mean_upper_bound=0.99,
            moving_average_window=7),
        InputSet(
            name="pct_sanitized_contained_at",
            full_lookback_window=90,
            range_test_window=4,
            range_lower_bound=0.125,
            range_upper_bound=0.875,
            mean_test_window=8,
            mean_lower_bound=0.025,
            mean_upper_bound=0.975,
            moving_average_window=7),
        InputSet(
            name="pct_sanitized_contained_numbers",
            full_lookback_window=90,
            range_test_window=3,
            range_lower_bound=0.075,
            range_upper_bound=0.925,
            mean_test_window=8,
            mean_lower_bound=0.01,
            mean_upper_bound=0.99,
            moving_average_window=7
        ),
        InputSet(
            name="pct_sanitized_contained_name",
            full_lookback_window=90,
            range_test_window=5,
            range_lower_bound=0.025,
            range_upper_bound=0.975,
            mean_test_window=7,
            mean_lower_bound=0.01,
            mean_upper_bound=0.99,
            moving_average_window=7),
        InputSet(
            name="pct_terms_containing_us_census_surname",
            full_lookback_window=90,
            range_test_window=3,
            range_lower_bound=0.1,
            range_upper_bound=0.9,
            mean_test_window=8,
            mean_lower_bound=0.01,
            mean_upper_bound=0.99,
            moving_average_window=9,
        ),
        InputSet(
            name="pct_uppercase_chars_all_search_terms",
            full_lookback_window=90,
            range_test_window=4,
            range_lower_bound=0.075,
            range_upper_bound=0.925,
            mean_test_window=8,
            mean_lower_bound=0.01,
            mean_upper_bound=0.99,
            moving_average_window=7
        ),
        InputSet(
            name="avg_words_all_search_terms",
            full_lookback_window=90,
            range_test_window=4,
            range_lower_bound=0.125,
            range_upper_bound=0.875,
            mean_test_window=8,
            mean_lower_bound=0.025,
            mean_upper_bound=0.975,
            moving_average_window=7),
        InputSet(
            name="pct_terms_non_english",
            full_lookback_window=90,
            range_test_window=4,
            range_lower_bound=0.125,
            range_upper_bound=0.875,
            mean_test_window=8,
            mean_lower_bound=0.01,
            mean_upper_bound=0.99,
            moving_average_window=5),
    ]:
        (
            finished_at,
            num_ranges_compared,
            range_alarm,
            range_low,
            range_high,
            range_test_vals,
        ) = range_check(
            val_df,
            metric.name,
            metric.full_lookback_window,
            metric.range_test_window,
            metric.range_lower_bound,
            metric.range_upper_bound,
        )
        (
            finished_at,
            num_moving_averages_compared,
            mean_alarm,
            mean_low,
            mean_high,
            mean_window,
            mean_test_vals,
        ) = mean_check(
            val_df,
            metric.name,
            metric.full_lookback_window,
            metric.mean_test_window,
            metric.moving_average_window,
            metric.mean_lower_bound,
            metric.mean_upper_bound,
        )

        rows_to_insert = [
            {
                "from_sanitization_job_finished_at": finished_at.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                "range_alarm": range_alarm,
                "range_low": range_low,
                "range_high": range_high,
                "num_ranges_compared": num_ranges_compared,
                "range_test_vals": str(range_test_vals),
                "mean_alarm": mean_alarm,
                "mean_low": mean_low,
                "mean_high": mean_high,
                "num_moving_averages_compared": num_moving_averages_compared,
                "mean_test_vals": str(mean_test_vals),
                "metric": metric.name,
                "full_lookback_window_num_days": metric.full_lookback_window,
                "range_test_window_num_days": metric.range_test_window,
                "mean_test_window_num_days": metric.mean_test_window,
                "moving_average_window_num_days": metric.moving_average_window,
                "range_percentile_lower_bound": metric.range_lower_bound,
                "range_percentile_upper_bound": metric.range_upper_bound,
                "mean_percentile_lower_bound": metric.range_lower_bound,
                "mean_percentile_upper_bound": metric.range_upper_bound,
            },
        ]
        errors = client.insert_rows_json(destination_table, rows_to_insert)
        if errors:
            print(f"Problem recording data validation results: {errors}")
        else:
            print("Data validation results recorded successfully!")
