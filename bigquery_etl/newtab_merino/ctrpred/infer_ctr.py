"""Vectorized inference for the 24-hour, seasonal log-odds ACTRSSM."""

from dataclasses import dataclass

import numpy as np


N_BUCKETS = 144
BUCKETS_PER_HOUR = 6
P_EPS = 1e-9

# Dimension key:
#   a = articles
#   n = observed 10-minute buckets (144)
#   h = UTC hours (24)
#   2 = clicks and adjusted exposure


@dataclass(frozen=True)
class ActrSsmConfig:
    global_ctr: float  # scalar
    hourly_ctr: np.ndarray  # [h]
    item_prior_exposure: float  # scalar
    phi: float  # scalar
    process_variance: float  # scalar
    initial_variance: float  # scalar
    newton_steps: int = 50  # scalar


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


def _poisson_log_posterior(
    x: np.ndarray,
    clicks: np.ndarray,
    exposure: np.ndarray,
    x_pred: np.ndarray,
    P_pred: np.ndarray,
) -> np.ndarray:
    """Evaluate the Poisson log posterior for each article."""
    log_p = -np.logaddexp(0.0, -x)  # [a]
    return (  # [a]
        -0.5 * (x - x_pred) ** 2 / P_pred
        + clicks * log_p
        - exposure * np.exp(log_p)
    )


def _laplace_poisson_update(
    clicks: np.ndarray,
    exposure: np.ndarray,
    x_pred: np.ndarray,
    P_pred: np.ndarray,
    newton_steps: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Find the posterior mode and local variance with Newton's method."""
    x = x_pred.copy()  # [a]
    active = exposure > 0  # [a]

    for _ in range(newton_steps):
        if not active.any():
            break

        p = _sigmoid(x)  # [a]
        g = -(x - x_pred) / P_pred + (clicks - exposure * p) * (1.0 - p)  # [a]
        precision = (  # [a]
            1.0 / P_pred
            + p * (1.0 - p) * (clicks + exposure * (1.0 - 2.0 * p))
        )
        fisher_precision = 1.0 / P_pred + exposure * p * (1.0 - p) ** 2  # [a]
        step = g / np.where(precision > 0, precision, fisher_precision)  # [a]
        step = np.clip(step, -5.0, 5.0)  # [a]
        before = _poisson_log_posterior(  # [a]
            x, clicks, exposure, x_pred, P_pred
        )

        accepted = ~active  # [a]
        candidate = x + step  # [a]
        for _ in range(40):
            value = _poisson_log_posterior(  # [a]
                candidate, clicks, exposure, x_pred, P_pred
            )
            accepted |= active & (value >= before - 1e-12)
            retry = active & ~accepted  # [a]
            if not retry.any():
                break
            step[retry] *= 0.5
            candidate[retry] = x[retry] + step[retry]
        else:
            raise RuntimeError("Poisson posterior line search failed")

        x[active] = candidate[active]
        active &= np.abs(step) >= 1e-8
    else:
        if active.any():
            raise RuntimeError("Poisson posterior mode did not converge")

    p = _sigmoid(x)  # [a]
    precision = (  # [a]
        1.0 / P_pred
        + p * (1.0 - p) * (clicks + exposure * (1.0 - 2.0 * p))
    )
    if (precision <= 0).any() or not np.isfinite(precision).all():
        raise RuntimeError("Poisson posterior has invalid local curvature")
    P = 1.0 / precision  # [a]
    return x, P


def predict_log_odds(
    counts: np.ndarray,
    forecast_slot: int,
    config: ActrSsmConfig,
) -> LogOddsPrediction:
    """Predict the next 10-minute log-odds ACTR for a batch of articles.

    ``counts[a, t]`` is ``(clicks, adjusted_exposure)`` for article ``a`` in
    one of the 144 ten-minute buckets immediately before ``forecast_slot``.
    ``forecast_slot`` is the UTC ten-minute slot of day, in ``[0, 143]``.
    """

    counts = np.asarray(counts, dtype=float)  # [a, n, 2]
    hourly_ctr = np.asarray(config.hourly_ctr, dtype=float)  # [h]
    assert counts.ndim == 3 and counts.shape[1:] == (N_BUCKETS, 2)
    assert np.isfinite(counts).all() and (counts >= 0).all()
    assert (counts[:, :, 0] == np.floor(counts[:, :, 0])).all()
    assert not ((counts[:, :, 1] == 0) & (counts[:, :, 0] > 0)).any()
    assert isinstance(forecast_slot, (int, np.integer)) and 0 <= forecast_slot < N_BUCKETS
    assert hourly_ctr.shape == (24,) and ((0 < hourly_ctr) & (hourly_ctr < 1)).all()
    assert 0 < config.global_ctr < 1 and config.item_prior_exposure > 0
    assert 0 <= config.phi <= 1 and config.process_variance >= 0
    assert config.initial_variance > 0 and config.newton_steps > 0

    clicks = counts[:, :, 0]  # [a, n]
    exposure = counts[:, :, 1]  # [a, n]
    n_articles = counts.shape[0]  # scalar = a

    # The time-varying center mu_t follows the hourly population ACTR.
    # Work backward from the forecast slot to recover the phase of all 144
    # observations; the first observation is exactly one day before forecast.
    slots = (forecast_slot - np.arange(N_BUCKETS, -1, -1)) % N_BUCKETS  # [n + 1]
    seasonal_mean = _logit(hourly_ctr[slots // BUCKETS_PER_HOUR])  # [n + 1]

    # At prediction time t, only observations strictly before bucket t are
    # known.  The last column contains the full 24-hour window.
    C = np.concatenate(  # [a, n + 1]
        (np.zeros((n_articles, 1)), np.cumsum(clicks, axis=1)), axis=1
    )
    A = np.concatenate(
        (np.zeros((n_articles, 1)), np.cumsum(exposure, axis=1)), axis=1
    )  # [a, n + 1]

    # The article-specific part of mu_t is a population-shrunk 24-hour ACTR,
    # expressed as a log-odds displacement from the global population ACTR.
    prior_A = config.item_prior_exposure  # scalar
    smooth_ctr = (C + prior_A * config.global_ctr) / (A + prior_A)  # [a, n + 1]
    item_offset = _logit(smooth_ctr) - _logit(  # [a, n + 1]
        np.asarray(config.global_ctr)
    )
    mu = seasonal_mean[None, :] + item_offset  # [a, n + 1]

    # x_t is the hidden log-odds ACTR.  Each column operation is one state-space
    # step; all articles make that step in parallel.
    x = mu[:, 0].copy()  # [a]
    P = np.full(n_articles, config.initial_variance)  # [a]
    previous_mu = mu[:, 0]  # [a]
    failed = np.zeros(n_articles, dtype=bool)  # [a]

    for t in range(N_BUCKETS):  # scalar t = 0, ..., n - 1
        x_pred = mu[:, t] + config.phi * (x - previous_mu)  # [a]
        P_pred = config.phi**2 * P + config.process_variance  # [a]
        healthy = ~failed  # [a]
        if healthy.any():
            try:
                x[healthy], P[healthy] = _laplace_poisson_update(
                    clicks[healthy, t],
                    exposure[healthy, t],
                    x_pred[healthy],
                    P_pred[healthy],
                    config.newton_steps,
                )
            except RuntimeError:
                # A numerical problem for one article should not discard the
                # predictions for the rest of the batch.
                for article in np.flatnonzero(healthy):
                    try:
                        x[article], P[article] = _laplace_poisson_update(
                            clicks[article : article + 1, t],
                            exposure[article : article + 1, t],
                            x_pred[article : article + 1],
                            P_pred[article : article + 1],
                            config.newton_steps,
                        )
                    except RuntimeError:
                        failed[article] = True
                        x[article] = np.nan
                        P[article] = np.nan
        previous_mu = mu[:, t]  # [a]

    # This is x^- for the next bucket: the prediction before its clicks exist.
    x_pred = mu[:, -1] + config.phi * (x - previous_mu)  # [a]
    P_pred = config.phi**2 * P + config.process_variance  # [a]

    available = (A[:, -1] > 0) & ~failed  # [a]
    x_pred = np.where(available, x_pred, np.nan)  # [a]
    P_pred = np.where(available, P_pred, np.nan)  # [a]
    return LogOddsPrediction(x_pred, P_pred, available)
