"""Tests for CTR prediction data shaping and artifact replacement."""

import numpy as np
import pandas as pd

from bigquery_etl.newtab_merino.ctrpred.pseudo_counts import PseudoCounts
from bigquery_etl.newtab_merino.ctrpred.utils import (
    build_model_input,
    replace_ctrpred_treatment_rows,
)


def test_build_model_input_packs_all_buckets_in_item_order():
    timeline = pd.DataFrame(
        [
            {
                "corpus_item_id": "item-b",
                "bucket": 0,
                "clicks": 2,
                "adjusted_impressions": 10.0,
            },
            {
                "corpus_item_id": "item-a",
                "bucket": 143,
                "clicks": 1,
                "adjusted_impressions": 5.0,
            },
        ]
    )

    item_ids, counts = build_model_input(timeline)

    assert item_ids.tolist() == ["item-b", "item-a"]
    assert counts.shape == (2, 144, 2)
    assert counts[0, 0].tolist() == [2.0, 10.0]
    assert counts[1, 143].tolist() == [1.0, 5.0]
    assert (counts.sum(axis=(1, 2)) > 0).all()


def test_replace_ctrpred_treatment_rows_preserves_other_rows():
    artifact = [
        {
            "corpus_item_id": "item-a",
            "click_count": 4,
            "impression_count": 100,
            "report_count": 2,
            "region": "GB-ctrpred_engb-treatment",
        },
        {
            "corpus_item_id": "item-a",
            "click_count": 8,
            "impression_count": 100,
            "report_count": 3,
            "region": "GB",
        },
    ]
    pseudo_counts = PseudoCounts(
        clicks=np.array([12.5]),
        impressions=np.array([250.0]),
    )

    replace_ctrpred_treatment_rows(
        artifact,
        [("item-a", "GB-ctrpred_engb-treatment")],
        pseudo_counts,
    )

    assert artifact[0]["click_count"] == 12.5
    assert artifact[0]["impression_count"] == 250.0
    assert artifact[0]["report_count"] == 2
    assert artifact[1]["click_count"] == 8
    assert artifact[1]["impression_count"] == 100
