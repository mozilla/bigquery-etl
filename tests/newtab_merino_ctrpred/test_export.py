"""Tests for the in-memory CTR prediction export path."""

import numpy as np
import pandas as pd

import bigquery_etl.newtab_merino as merino
from bigquery_etl.newtab_merino.ctrpred.config import GB_CTRPRED_CONFIG


def test_apply_ctrpred_postprocessing_replaces_treatment_rows(monkeypatch):
    artifact = [
        {
            "corpus_item_id": "item-a",
            "click_count": 4,
            "impression_count": 100,
            "report_count": 2,
            "region": merino.CTR_PRED_TREATMENT_REGION,
        },
        {
            "corpus_item_id": "item-a",
            "click_count": 8,
            "impression_count": 100,
            "report_count": 3,
            "region": "GB",
        },
    ]
    timeline = pd.DataFrame(
        {
            "corpus_item_id": ["item-a"] * 144,
            "bucket": np.arange(144),
            "clicks": [1] * 144,
            "adjusted_impressions": [20.0] * 144,
        }
    )

    def fake_query_timeline_data(
        client, corpus_item_ids, end_time, region, experiment_slug, experiment_branch
    ):
        assert corpus_item_ids == ["item-a"]
        assert end_time == pd.Timestamp("2026-09-14T20:00:00Z").to_pydatetime()
        assert (region, experiment_slug, experiment_branch) == (
            GB_CTRPRED_CONFIG.region,
            GB_CTRPRED_CONFIG.experiment_slug,
            GB_CTRPRED_CONFIG.experiment_branch,
        )
        return timeline

    monkeypatch.setattr(merino, "query_timeline_data", fake_query_timeline_data)

    end_time = pd.Timestamp("2026-09-14T20:00:00Z").to_pydatetime()
    result = merino.apply_ctrpred_postprocessing(
        artifact, client=None, end_time=end_time
    )

    treatment = result[0]
    assert np.isfinite(treatment["click_count"])
    assert np.isfinite(treatment["impression_count"])
    assert 0 <= treatment["click_count"] <= treatment["impression_count"]
    assert treatment["report_count"] == 2
    assert result[1]["click_count"] == 8
    assert result[1]["impression_count"] == 100


def test_apply_ctrpred_postprocessing_returns_original_rows_on_failure(monkeypatch):
    artifact = [{"region": merino.CTR_PRED_TREATMENT_REGION}]

    def fail(*args):
        raise RuntimeError("test failure")

    monkeypatch.setattr(merino, "query_timeline_data", fail)

    assert (
        merino.apply_ctrpred_postprocessing(artifact, client=None, end_time=None)
        is artifact
    )


def test_apply_ctrpred_postprocessing_returns_original_rows_for_invalid_artifact(
    monkeypatch,
):
    artifact = [None]

    monkeypatch.setattr(merino, "query_timeline_data", lambda *args: None)

    assert (
        merino.apply_ctrpred_postprocessing(artifact, client=None, end_time=None)
        is artifact
    )


def test_apply_ctrpred_postprocessing_skips_empty_treatment(monkeypatch):
    artifact = [{"click_count": 1}]

    def fail(*args):
        raise AssertionError("timeline query should not run")

    monkeypatch.setattr(merino, "query_timeline_data", fail)

    assert (
        merino.apply_ctrpred_postprocessing(artifact, client=None, end_time=None)
        is artifact
    )
