"""Tests for the parameterized BigQuery timeline query."""

from unittest.mock import Mock

import pandas as pd

from bigquery_etl.newtab_merino.ctrpred.timeline import (
    TIMELINE_QUERY,
    TIMELINE_QUERY_TIMEOUT_SECONDS,
    query_timeline_data,
)


def test_query_timeline_data_binds_item_and_stratification_parameters():
    client = Mock()
    client.query.return_value.result.return_value.to_dataframe.return_value = (
        pd.DataFrame()
    )

    result = query_timeline_data(
        client,
        ["item-a", "item-b"],
        end_time="2026-09-14T20:00:00+00:00",
        region="GB",
        experiment_slug="ctrpred_engb",
        experiment_branch="treatment",
    )

    assert result.empty
    (query,) = client.query.call_args.args
    job_config = client.query.call_args.kwargs["job_config"]
    client.query.return_value.result.assert_called_once_with(
        timeout=TIMELINE_QUERY_TIMEOUT_SECONDS
    )
    parameters = {
        parameter.name: parameter for parameter in job_config.query_parameters
    }

    assert query == TIMELINE_QUERY
    assert parameters["corpus_item_ids"].values == ["item-a", "item-b"]
    assert (
        parameters["end_time"].value
        == pd.Timestamp("2026-09-14T20:00:00Z").to_pydatetime()
    )
    assert parameters["region"].value == "GB"
    assert parameters["experiment_slug"].value == "ctrpred_engb"
    assert parameters["experiment_branch"].value == "treatment"
