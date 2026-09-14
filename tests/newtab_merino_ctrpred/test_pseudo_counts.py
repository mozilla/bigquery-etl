"""Tests for converting log-odds forecasts to pseudo-counts."""

import numpy as np

from bigquery_etl.newtab_merino.ctrpred.pseudo_counts import (
    PseudoCountConfig,
    log_odds_to_pseudo_counts,
)


def test_log_odds_to_pseudo_counts_returns_finite_counts():
    counts = log_odds_to_pseudo_counts(
        mean=np.array([-2.0, -4.0]),
        variance=np.array([0.2, 0.5]),
        config=PseudoCountConfig(strength_multiplier=32.0),
    )

    assert np.isfinite(counts.clicks).all()
    assert np.isfinite(counts.impressions).all()
    assert (counts.clicks >= 0).all()
    assert (counts.clicks <= counts.impressions).all()


def test_log_odds_to_pseudo_counts_preserves_unavailable_predictions_as_nan():
    counts = log_odds_to_pseudo_counts(
        mean=np.array([np.nan]),
        variance=np.array([np.nan]),
    )

    assert np.isnan(counts.clicks).all()
    assert np.isnan(counts.impressions).all()
