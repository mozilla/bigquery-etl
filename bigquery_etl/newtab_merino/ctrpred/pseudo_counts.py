"""Convert a Gaussian log-odds forecast to continuous Beta pseudo-counts."""

from dataclasses import dataclass

import numpy as np

P_EPS = 1e-9

# Dimension key:
#   a = articles


@dataclass(frozen=True)
class PseudoCountConfig:
    """Parameters controlling the strength of generated pseudo-counts."""

    strength_multiplier: float = 1.0  # scalar
    min_strength: float = 1.0  # scalar
    max_strength: float = 10_000_000.0  # scalar


@dataclass(frozen=True)
class PseudoCounts:
    """Pseudo clicks and impressions corresponding to each prediction."""

    clicks: np.ndarray  # [a]
    impressions: np.ndarray  # [a]


def _sigmoid(x: np.ndarray) -> np.ndarray:
    y = np.empty_like(x, dtype=float)  # [same as x]
    pos = x >= 0  # [same as x]
    y[pos] = 1.0 / (1.0 + np.exp(-x[pos]))
    ex = np.exp(x[~pos])  # [negative elements of x]
    y[~pos] = ex / (1.0 + ex)
    return y


def log_odds_to_pseudo_counts(
    mean: np.ndarray,
    variance: np.ndarray,
    config: PseudoCountConfig = PseudoCountConfig(),
) -> PseudoCounts:
    """Moment-match log-odds forecasts to Beta clicks and impressions."""
    mean = np.asarray(mean, dtype=float)  # [a]
    variance = np.asarray(variance, dtype=float)  # [a]
    assert mean.ndim == 1 and mean.shape == variance.shape
    assert config.min_strength > 0
    assert config.max_strength >= config.min_strength
    assert config.strength_multiplier > 0

    # The latent state is Gaussian in log-odds.  The sigmoid maps its mean to
    # CTR, and the delta method maps its variance into CTR space.
    p = np.clip(_sigmoid(mean), P_EPS, 1.0 - P_EPS)  # [a]
    valid = np.isfinite(mean) & np.isfinite(variance) & (variance > 0)  # [a]
    var_p = (p * (1.0 - p)) ** 2 * variance  # [a]

    # A Beta distribution with mean p and total concentration k has variance
    # p(1-p)/(k+1).  Equating that variance with var_p determines k.
    k = np.full_like(p, np.nan)  # [a]
    np.divide(p * (1.0 - p), var_p, out=k, where=valid)
    k -= 1.0
    k = np.clip(k, config.min_strength, config.max_strength)  # [a]
    k = np.clip(  # [a]
        k * config.strength_multiplier,
        config.min_strength,
        config.max_strength,
    )

    pseudo_impressions = np.where(valid, k, np.nan)  # [a]
    pseudo_clicks = np.where(valid, p * k, np.nan)  # [a]
    return PseudoCounts(pseudo_clicks, pseudo_impressions)
