"""Tests for the in-memory CTR prediction export path."""

import numpy as np
import pandas as pd

import bigquery_etl.newtab_merino as merino


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
        client, corpus_item_ids, region, experiment_slug, experiment_branch
    ):
        assert corpus_item_ids == ["item-a"]
        assert (region, experiment_slug, experiment_branch) == (
            "GB",
            "ctrpred_engb",
            "treatment",
        )
        return timeline

    monkeypatch.setattr(merino, "query_timeline_data", fake_query_timeline_data)

    result = merino.apply_ctrpred_postprocessing(artifact, client=None)

    treatment = result[0]
    assert np.isfinite(treatment["click_count"])
    assert np.isfinite(treatment["impression_count"])
    assert 0 <= treatment["click_count"] <= treatment["impression_count"]
    assert treatment["report_count"] == 2
    assert result[1]["click_count"] == 8
    assert result[1]["impression_count"] == 100
