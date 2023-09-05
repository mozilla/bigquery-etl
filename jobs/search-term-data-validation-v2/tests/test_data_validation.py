import pytest
from search_term_data_validation_v2.data_validation import range_check, mean_check
import pandas as pd
import numpy as np


def test_range_check__wrong_format_lower_bound():
    example_df = pd.DataFrame({})

    try:
        result = range_check(
            validation_data=example_df,
            metric="column_name",
            full_lookback_window=3,
            test_window=1,
            range_lower_bound=25,
            range_upper_bound=0.75,
        )
    except Exception as e:
        assert (
            str(e)
            == "range_lower_bound and range_upper_bound should both be between zero (0) and one (1)."
        )


def test_range_check__wrong_format_upper_bound():
    example_df = pd.DataFrame({})

    try:
        result = range_check(
            validation_data=example_df,
            metric="column_name",
            full_lookback_window=3,
            test_window=1,
            range_lower_bound=0.25,
            range_upper_bound=75,
        )
    except Exception as e:
        assert (
            str(e)
            == "range_lower_bound and range_upper_bound should both be between zero (0) and one (1)."
        )


def test_range_check__no_finished_at_column():
    example_df = pd.DataFrame({})

    try:
        result = range_check(
            validation_data=example_df,
            metric="column_name",
            full_lookback_window=3,
            test_window=1,
            range_lower_bound=0.25,
            range_upper_bound=0.75,
        )
    except Exception as e:
        assert str(e) == "dataframe must include a finished_at column."


def test_range_check__target_metric_not_present():
    example_df = pd.DataFrame({"finished_at": []})

    try:
        result = range_check(
            validation_data=example_df,
            metric="column_that_is_not_in_df",
            full_lookback_window=3,
            test_window=1,
            range_lower_bound=0.25,
            range_upper_bound=0.75,
        )
    except Exception as e:
        assert (
            str(e)
            == 'dataframe does not include target metric "column_that_is_not_in_df"'
        )


def test_range_check__happy_path__test_metric_in_range():
    example_df = pd.DataFrame(
        {
            "finished_at": [
                np.datetime64("today", "D") - np.timedelta64(12, "D"),
                np.datetime64("today", "D") - np.timedelta64(11, "D"),
                np.datetime64("today", "D") - np.timedelta64(10, "D"),
                np.datetime64("today", "D") - np.timedelta64(9, "D"),
                np.datetime64("today", "D") - np.timedelta64(8, "D"),
                np.datetime64("today", "D") - np.timedelta64(7, "D"),
                np.datetime64("today", "D") - np.timedelta64(6, "D"),
                np.datetime64("today", "D") - np.timedelta64(5, "D"),
                np.datetime64("today", "D") - np.timedelta64(4, "D"),
                np.datetime64("today", "D") - np.timedelta64(3, "D"),
                np.datetime64("today", "D") - np.timedelta64(2, "D"),
                np.datetime64("today", "D") - np.timedelta64(1, "D"),
                np.datetime64("today", "D") - np.timedelta64(0, "D"),
            ],
            "pct_something_consistent": [10, 9, 9, 8, 7, 6, 5, 6, 7, 8, 6, 9, 7],
        }
    )

    result = range_check(
        validation_data=example_df,
        metric="pct_something_consistent",
        full_lookback_window=12,
        test_window=1,
        range_lower_bound=0.2,
        range_upper_bound=0.8,
    )
    (
        latest_timestamp,
        num_compared,
        should_alarm,
        lower_bound,
        upper_bound,
        test_values,
    ) = result

    assert num_compared == 12
    assert should_alarm == False
    assert lower_bound == 6.0
    assert upper_bound == 9.0
    assert test_values == [7]


def test_range_check__happy_path__test_metric_out_of_range():
    example_df = pd.DataFrame(
        {
            "finished_at": [
                np.datetime64("today", "D") - np.timedelta64(12, "D"),
                np.datetime64("today", "D") - np.timedelta64(11, "D"),
                np.datetime64("today", "D") - np.timedelta64(10, "D"),
                np.datetime64("today", "D") - np.timedelta64(9, "D"),
                np.datetime64("today", "D") - np.timedelta64(8, "D"),
                np.datetime64("today", "D") - np.timedelta64(7, "D"),
                np.datetime64("today", "D") - np.timedelta64(6, "D"),
                np.datetime64("today", "D") - np.timedelta64(5, "D"),
                np.datetime64("today", "D") - np.timedelta64(4, "D"),
                np.datetime64("today", "D") - np.timedelta64(3, "D"),
                np.datetime64("today", "D") - np.timedelta64(2, "D"),
                np.datetime64("today", "D") - np.timedelta64(1, "D"),
                np.datetime64("today", "D") - np.timedelta64(0, "D"),
            ],
            "pct_something_consistent": [10, 9, 9, 8, 7, 6, 5, 6, 7, 8, 6, 9, 3],
        }
    )

    result = range_check(
        validation_data=example_df,
        metric="pct_something_consistent",
        full_lookback_window=12,
        test_window=1,
        range_lower_bound=0.2,
        range_upper_bound=0.8,
    )
    (
        latest_timestamp,
        num_compared,
        should_alarm,
        lower_bound,
        upper_bound,
        test_values,
    ) = result

    assert num_compared == 12
    assert should_alarm == True
    assert lower_bound == 6.0
    assert upper_bound == 9.0
    assert test_values == [3]


def test_mean_check__wrong_format_lower_bound():
    example_df = pd.DataFrame({})

    try:
        result = mean_check(
            validation_data=example_df,
            metric="column_name",
            full_lookback_window=3,
            test_window=1,
            moving_average_window=1,
            mean_lower_bound=25,
            mean_upper_bound=0.75,
        )
    except Exception as e:
        assert (
            str(e)
            == "mean_lower_bound and mean_upper_bound should both be between zero (0) and one (1)."
        )


def test_mean_check__wrong_format_upper_bound():
    example_df = pd.DataFrame({})

    try:
        result = mean_check(
            validation_data=example_df,
            metric="column_name",
            full_lookback_window=3,
            test_window=1,
            moving_average_window=1,
            mean_lower_bound=0.25,
            mean_upper_bound=75,
        )
    except Exception as e:
        assert (
            str(e)
            == "mean_lower_bound and mean_upper_bound should both be between zero (0) and one (1)."
        )


def test_mean_check__no_finished_at_column():
    example_df = pd.DataFrame({})

    try:
        result = mean_check(
            validation_data=example_df,
            metric="column_name",
            full_lookback_window=3,
            test_window=1,
            moving_average_window=1,
            mean_lower_bound=0.25,
            mean_upper_bound=0.75,
        )
    except Exception as e:
        assert str(e) == "dataframe must include a finished_at column."


def test_mean_check__target_metric_not_present():
    example_df = pd.DataFrame({"finished_at": []})

    try:
        result = mean_check(
            validation_data=example_df,
            metric="column_that_is_not_in_df",
            full_lookback_window=3,
            test_window=1,
            moving_average_window=1,
            mean_lower_bound=0.25,
            mean_upper_bound=0.75,
        )
    except Exception as e:
        assert (
            str(e)
            == 'dataframe does not include target metric "column_that_is_not_in_df"'
        )


def test_mean_check__happy_path__test_metric_in_moving_average_range():
    example_df = pd.DataFrame(
        {
            "finished_at": [
                np.datetime64("today", "D") - np.timedelta64(12, "D"),
                np.datetime64("today", "D") - np.timedelta64(11, "D"),
                np.datetime64("today", "D") - np.timedelta64(10, "D"),
                np.datetime64("today", "D") - np.timedelta64(9, "D"),
                np.datetime64("today", "D") - np.timedelta64(8, "D"),
                np.datetime64("today", "D") - np.timedelta64(7, "D"),
                np.datetime64("today", "D") - np.timedelta64(6, "D"),
                np.datetime64("today", "D") - np.timedelta64(5, "D"),
                np.datetime64("today", "D") - np.timedelta64(4, "D"),
                np.datetime64("today", "D") - np.timedelta64(3, "D"),
                np.datetime64("today", "D") - np.timedelta64(2, "D"),
                np.datetime64("today", "D") - np.timedelta64(1, "D"),
                np.datetime64("today", "D") - np.timedelta64(0, "D"),
            ],
            "pct_something_consistent": [10, 9, 9, 8, 7, 6, 5, 6, 7, 8, 6, 9, 7],
        }
    )

    result = mean_check(
        validation_data=example_df,
        metric="pct_something_consistent",
        full_lookback_window=12,
        test_window=1,
        moving_average_window=3,
        mean_lower_bound=0.2,
        mean_upper_bound=0.8,
    )
    (
        latest_timestamp,
        num_compared,
        should_alarm,
        lower_bound,
        upper_bound,
        moving_average_window,
        test_values,
    ) = result

    assert num_compared == 12
    assert should_alarm == False
    assert lower_bound == 6.2
    assert upper_bound == 9.200000000000001
    assert test_values == [7.333333333333333]


def test_mean_check__happy_path__test_metric_out_of_moving_average_range():
    example_df = pd.DataFrame(
        {
            "finished_at": [
                np.datetime64("today", "D") - np.timedelta64(12, "D"),
                np.datetime64("today", "D") - np.timedelta64(11, "D"),
                np.datetime64("today", "D") - np.timedelta64(10, "D"),
                np.datetime64("today", "D") - np.timedelta64(9, "D"),
                np.datetime64("today", "D") - np.timedelta64(8, "D"),
                np.datetime64("today", "D") - np.timedelta64(7, "D"),
                np.datetime64("today", "D") - np.timedelta64(6, "D"),
                np.datetime64("today", "D") - np.timedelta64(5, "D"),
                np.datetime64("today", "D") - np.timedelta64(4, "D"),
                np.datetime64("today", "D") - np.timedelta64(3, "D"),
                np.datetime64("today", "D") - np.timedelta64(2, "D"),
                np.datetime64("today", "D") - np.timedelta64(1, "D"),
                np.datetime64("today", "D") - np.timedelta64(0, "D"),
            ],
            "pct_something_consistent": [10, 9, 9, 8, 7, 6, 5, 6, 7, 8, 6, 9, 3],
        }
    )

    result = mean_check(
        validation_data=example_df,
        metric="pct_something_consistent",
        full_lookback_window=12,
        test_window=1,
        moving_average_window=3,
        mean_lower_bound=0.2,
        mean_upper_bound=0.8,
    )
    (
        latest_timestamp,
        num_compared,
        should_alarm,
        lower_bound,
        upper_bound,
        moving_average_window,
        test_values,
    ) = result

    assert num_compared == 12
    assert should_alarm == True
    assert lower_bound == 6.2
    assert upper_bound == 9.200000000000001
    assert test_values == [6.0]
