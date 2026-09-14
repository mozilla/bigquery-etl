"""Vectorized inference for the 24-hour, seasonal log-odds CTRSSM."""

from dataclasses import dataclass

import numpy as np


N_BUCKETS = 144
BUCKETS_PER_HOUR = 6
P_EPS = 1e-9

# Dimension key:
#   a = articles
#   n = observed 10-minute buckets (144)
#   h = UTC hours (24)
#   2 = clicks and adjusted impressions


@dataclass(frozen=True)
class CtrSsmConfig:
    global_ctr: float  # scalar
    hourly_ctr: np.ndarray  # [h]
    item_prior_impressions: float  # scalar
    phi: float  # scalar
    process_variance: float  # scalar
    initial_variance: float  # scalar
    newton_steps: int = 8  # scalar


@dataclass(frozen=True)
class LogOddsPrediction:
    mean: np.ndarray  # [a]
    variance: np.ndarray  # [a]
    available: np.ndarray  # [a]


def _sigmoid(x: np.ndarray) -> np.ndarray:
    y = np.empty_like(x, dtype=float)  # [same as x]
    pos = x >= 0  # [same as x]
    y[pos] = 1.0 / (1.0 + np.exp(-x[pos]))
    ex = np.exp(x[~pos])  # [negative elements of x]
    y[~pos] = ex / (1.0 + ex)
    return y


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, P_EPS, 1.0 - P_EPS)  # [same as input p]
    return np.log(p / (1.0 - p))


def predict_log_odds(
    counts: np.ndarray,
    forecast_slot: int,
    config: CtrSsmConfig,
) -> LogOddsPrediction:
    """Predict the next 10-minute log-odds CTR for a batch of articles.

    ``counts[a, t]`` is ``(clicks, adjusted_impressions)`` for article ``a``
    in one of the 144 ten-minute buckets immediately before ``forecast_slot``.
    ``forecast_slot`` is the UTC ten-minute slot of day, in ``[0, 143]``.
    """

    counts = np.asarray(counts, dtype=float)  # [a, n, 2]
    hourly_ctr = np.asarray(config.hourly_ctr, dtype=float)  # [h]
    assert counts.ndim == 3 and counts.shape[1:] == (N_BUCKETS, 2)
    assert np.isfinite(counts).all()
    assert (counts >= 0).all() and (counts[:, :, 0] <= counts[:, :, 1]).all()
    assert isinstance(forecast_slot, (int, np.integer)) and 0 <= forecast_slot < N_BUCKETS
    assert hourly_ctr.shape == (24,) and ((0 < hourly_ctr) & (hourly_ctr < 1)).all()
    assert 0 < config.global_ctr < 1 and config.item_prior_impressions > 0
    assert 0 <= config.phi <= 1 and config.process_variance >= 0
    assert config.initial_variance > 0 and config.newton_steps > 0

    clicks = counts[:, :, 0]  # [a, n]
    impressions = counts[:, :, 1]  # [a, n]
    n_articles = counts.shape[0]  # scalar = a

    # The time-varying center mu_t follows the hourly population CTR.
    # Work backward from the forecast slot to recover the phase of all 144
    # observations; the first observation is exactly one day before forecast.
    slots = (forecast_slot - np.arange(N_BUCKETS, -1, -1)) % N_BUCKETS  # [n + 1]
    seasonal_mean = _logit(hourly_ctr[slots // BUCKETS_PER_HOUR])  # [n + 1]

    # At prediction time t, only observations strictly before bucket t are
    # known.  The leading zero column is therefore the causal history for the
    # first prediction, while the last column contains the full 24-hour window.
    C = np.concatenate(  # [a, n + 1]
        (np.zeros((n_articles, 1)), np.cumsum(clicks, axis=1)), axis=1
    )
    I = np.concatenate(
        (np.zeros((n_articles, 1)), np.cumsum(impressions, axis=1)), axis=1
    )  # [a, n + 1]

    # The article-specific part of mu_t is a population-shrunk 24-hour CTR,
    # expressed as a log-odds displacement from the global population CTR.
    prior_I = config.item_prior_impressions  # scalar
    smooth_ctr = (C + prior_I * config.global_ctr) / (I + prior_I)  # [a, n + 1]
    item_offset = _logit(smooth_ctr) - _logit(  # [a, n + 1]
        np.asarray(config.global_ctr)
    )
    mu = seasonal_mean[None, :] + item_offset  # [a, n + 1]

    # x_t is the hidden log-odds CTR.  Each column operation is one state-space
    # step; all articles make that step in parallel.
    x = mu[:, 0].copy()  # [a]
    P = np.full(n_articles, config.initial_variance)  # [a]
    previous_mu = mu[:, 0]  # [a]

    for t in range(N_BUCKETS):  # scalar t = 0, ..., n - 1
        x_pred = mu[:, t] + config.phi * (x - previous_mu)  # [a]
        P_pred = config.phi**2 * P + config.process_variance  # [a]

        # The Binomial observation is not Gaussian in log-odds space.  Newton's
        # method finds the posterior mode; curvature there gives its variance.
        x = x_pred.copy()  # [a]
        for _ in range(config.newton_steps):
            p = _sigmoid(x)  # [a]
            g = -(x - x_pred) / P_pred + clicks[:, t] - impressions[:, t] * p  # [a]
            H = -1.0 / P_pred - impressions[:, t] * p * (1.0 - p)  # [a]
            x -= g / H  # [a]

        p = _sigmoid(x)  # [a]
        P = 1.0 / (
            1.0 / P_pred + impressions[:, t] * p * (1.0 - p)
        )  # [a]
        previous_mu = mu[:, t]  # [a]

    # This is x^- for the next bucket: the prediction before its clicks exist.
    x_pred = mu[:, -1] + config.phi * (x - previous_mu)  # [a]
    P_pred = config.phi**2 * P + config.process_variance  # [a]

    available = I[:, -1] > 0  # [a]
    x_pred = np.where(available, x_pred, np.nan)  # [a]
    P_pred = np.where(available, P_pred, np.nan)  # [a]
    return LogOddsPrediction(x_pred, P_pred, available)
