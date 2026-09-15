"""Tests for the ACTRSSM log-odds prediction."""

import numpy as np

from bigquery_etl.newtab_merino.ctrpred.config import GB_CTRPRED_CONFIG
from bigquery_etl.newtab_merino.ctrpred.infer_ctr import N_BUCKETS, predict_log_odds


def test_predict_log_odds_returns_one_prediction_per_item():
    counts = np.zeros((2, N_BUCKETS, 2))
    counts[:, :, 0] = 1
    counts[:, :, 1] = 20

    prediction = predict_log_odds(
        counts, forecast_slot=72, config=GB_CTRPRED_CONFIG.actr
    )

    assert prediction.mean.shape == (2,)
    assert prediction.variance.shape == (2,)
    assert prediction.available.tolist() == [True, True]
    assert np.isfinite(prediction.mean).all()
    assert np.isfinite(prediction.variance).all()


def test_predict_log_odds_marks_items_without_exposure_unavailable():
    counts = np.zeros((1, N_BUCKETS, 2))

    prediction = predict_log_odds(
        counts, forecast_slot=72, config=GB_CTRPRED_CONFIG.actr
    )

    assert prediction.available.tolist() == [False]
    assert np.isnan(prediction.mean).all()
    assert np.isnan(prediction.variance).all()
