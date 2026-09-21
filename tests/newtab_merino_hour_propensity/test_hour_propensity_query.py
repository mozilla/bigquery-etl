"""Tests for newtab_merino_hour_propensity_v1 query.py (UTC-hour weights)."""

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Import the query module from its file path.
_repo_root = Path(__file__).resolve().parent.parent.parent
_query_path = (
    _repo_root
    / "sql/moz-fx-data-shared-prod/telemetry_derived/newtab_merino_hour_propensity_v1/query.py"
)

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not _query_path.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)

# Load the module dynamically since the path contains hyphens.
spec = importlib.util.spec_from_file_location(
    "newtab_merino_hour_propensity_query", _query_path
)
assert spec is not None and spec.loader is not None
query_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query_mod)

blend_weights = query_mod.blend_weights
compute_weights = query_mod.compute_weights
compute_all_countries = query_mod.compute_all_countries
normalize_weights = query_mod.normalize_weights


def _hist(rows):
    """Build a (country, hour) exposure frame from tuples.

    Each row is (country, hour, impressions, adjusted_impressions, clicks).
    """
    return pd.DataFrame(
        [
            {
                "country": country,
                "hour": hour,
                "impressions": impressions,
                "adjusted_impressions": float(adjusted),
                "clicks": clicks,
            }
            for country, hour, impressions, adjusted, clicks in rows
        ]
    )


def _flat_hist(country, ctr_by_hour, impressions=1_000_000, position_mult=1.0):
    """A full 24-hour history for one country with the given per-hour CTRs."""
    adjusted = impressions * position_mult
    return _hist(
        [
            (country, hour, impressions, adjusted, round(adjusted * ctr))
            for hour, ctr in ctr_by_hour.items()
        ]
    )


def _weights_df(rows):
    """Build an output-shaped weights DataFrame from (hour, imp, adj, clicks, w) rows."""
    return pd.DataFrame(
        [
            {
                "hour": hour,
                "impressions": imp,
                "adjusted_impressions": float(adj),
                "clicks": clicks,
                "weight": weight,
            }
            for hour, imp, adj, clicks, weight in rows
        ]
    )


@pytest.fixture
def identity_normalize(monkeypatch):
    """Neuter the re-normalization so we can assert the raw shrinkage math."""
    monkeypatch.setattr(
        query_mod,
        "normalize_weights",
        lambda weights: (weights["unnormalized_weight"], 1.0),
    )


def _swing_ctr(hour):
    """A 1.94x CTR swing with the measured GB panel's endpoints and turning points."""
    low, high = 0.00359, 0.00695  # hour 1 and hour 6 respectively
    hours_past_trough = (hour - 1) % 24
    # Rise over the five hours to the peak, then decay back over the other nineteen.
    if hours_past_trough <= 5:
        fraction = hours_past_trough / 5
    else:
        fraction = (24 - hours_past_trough) / 19
    return low + (high - low) * fraction


SWINGY_CTR = {hour: _swing_ctr(hour) for hour in range(24)}


def test_weight_is_inverse_of_relative_ctr():
    # The low-CTR hour must get a weight above 1 (its exposure shrinks) and the
    # high-CTR hour a weight below 1.
    out = compute_weights(_flat_hist("GB", SWINGY_CTR)).set_index("hour")
    assert out.loc[1, "weight"] > 1.0
    assert out.loc[6, "weight"] < 1.0
    # Weight tracks 1 / (ctr / global_ctr), so the ratio of weights is the inverse
    # ratio of CTRs.
    assert out.loc[1, "weight"] / out.loc[6, "weight"] == pytest.approx(
        SWINGY_CTR[6] / SWINGY_CTR[1], rel=1e-6
    )


def test_normalization_conserves_adjusted_exposure():
    # Acceptance test 1: SUM(adjusted_impressions / weight) == SUM(adjusted_impressions),
    # so overall CTR is preserved rather than rescaled.
    out = compute_weights(_flat_hist("GB", SWINGY_CTR))
    exposure = out["adjusted_impressions"].sum()
    reweighted = (out["adjusted_impressions"] / out["weight"]).sum()
    assert reweighted == pytest.approx(exposure, rel=1e-9)


def test_aa_ctr_is_flat_by_hour():
    # Acceptance test 2: after re-weighting, CTR per hour is flat (by construction on
    # the fit window) and equal to the global position-adjusted CTR.
    hist = _flat_hist("GB", SWINGY_CTR)
    out = compute_weights(hist)
    global_ctr = hist["clicks"].sum() / hist["adjusted_impressions"].sum()
    aa_ctr = out["clicks"] / (out["adjusted_impressions"] / out["weight"])
    assert aa_ctr.min() == pytest.approx(global_ctr, rel=1e-4)
    assert aa_ctr.max() == pytest.approx(global_ctr, rel=1e-4)


def test_estimated_on_adjusted_not_raw_exposure():
    # Hours 0-11 are served in cheap slots (position weight 2.0, so adjusted exposure is
    # half of raw) and hours 12-23 in expensive ones, but the underlying
    # position-adjusted CTR is identical everywhere. Estimating on raw impressions
    # would invent an hour effect; estimating on adjusted exposure must not.
    rows = []
    for hour in range(24):
        raw = 1_000_000
        adjusted = raw / 2.0 if hour < 12 else float(raw)
        rows.append(("GB", hour, raw, adjusted, round(adjusted * 0.005)))
    out = compute_weights(_hist(rows))
    assert out["weight"].min() == pytest.approx(1.0, rel=1e-3)
    assert out["weight"].max() == pytest.approx(1.0, rel=1e-3)


def test_thin_hours_are_dropped(monkeypatch):
    # Cells below MIN_CELL_IMPRESSIONS are not emitted; consumers fall back to global.
    monkeypatch.setattr(query_mod, "MIN_CELL_IMPRESSIONS", 10_000)
    hist = _flat_hist("GB", {hour: 0.005 for hour in range(24)})
    hist.loc[hist["hour"] == 3, ["impressions", "adjusted_impressions", "clicks"]] = [
        500,
        500.0,
        2,
    ]
    out = compute_weights(hist)
    assert 3 not in set(out["hour"])
    assert len(out) == 23


def test_hours_without_clicks_are_dropped():
    # A zero-click hour has an undefined multiplier; it must be dropped, not emitted
    # as an infinite weight.
    hist = _flat_hist("GB", {hour: 0.005 for hour in range(24)})
    hist.loc[hist["hour"] == 4, "clicks"] = 0
    out = compute_weights(hist)
    assert 4 not in set(out["hour"])
    assert np.isfinite(out["weight"]).all()
    assert (out["weight"] > 0).all()


def test_compute_weights_empty_without_clicks():
    # No clicks anywhere -> no weights, no exception.
    hist = _flat_hist("GB", {hour: 0.0 for hour in range(24)})
    assert compute_weights(hist).empty


def test_output_shape():
    out = compute_weights(_flat_hist("GB", SWINGY_CTR))
    assert list(out.columns) == [
        "hour",
        "impressions",
        "adjusted_impressions",
        "clicks",
        "weight",
    ]
    assert out["hour"].tolist() == list(range(24))


def test_blend_high_volume_stays_country(identity_normalize):
    # imp_c >> K -> blended weight is essentially the country weight.
    country = _weights_df([(1, 10_000_000, 10_000_000.0, 50_000, 2.0)])
    glob = _weights_df([(1, 1_000, 1_000.0, 5, 0.5)])
    out = blend_weights(country, glob, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx(2.0, rel=0.01)


def test_blend_low_volume_leans_global(identity_normalize):
    # imp_c -> 0 -> blended weight collapses to the global weight.
    country = _weights_df([(1, 0, 0.0, 0, 2.0)])
    glob = _weights_df([(1, 1_000, 1_000.0, 5, 0.5)])
    out = blend_weights(country, glob, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx(0.5, rel=1e-6)


def test_blend_midpoint(identity_normalize):
    # imp_c == K -> exact 50/50 average of country and global weights.
    country = _weights_df([(1, 50_000, 50_000.0, 250, 2.0)])
    glob = _weights_df([(1, 1_000, 1_000.0, 5, 0.5)])
    out = blend_weights(country, glob, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx((2.0 + 0.5) / 2, rel=1e-6)


def test_blend_missing_global_hour_falls_back_to_country(identity_normalize):
    # Hour present for the country but not global -> uses the country weight.
    country = _weights_df([(7, 100_000, 100_000.0, 500, 1.7)])
    glob = _weights_df([(1, 1_000, 1_000.0, 5, 0.5)])
    out = blend_weights(country, glob, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx(1.7, rel=1e-6)


def test_blend_renormalizes_to_country_exposure():
    # Without the identity_normalize fixture the blended set must still conserve the
    # country's own adjusted exposure.
    country = compute_weights(_flat_hist("GB", SWINGY_CTR))
    glob = compute_weights(_flat_hist("GB", {hour: 0.005 for hour in range(24)}))
    out = blend_weights(country, glob, k=50_000)
    exposure = out["adjusted_impressions"].sum()
    reweighted = (out["adjusted_impressions"] / out["weight"]).sum()
    assert reweighted == pytest.approx(exposure, rel=1e-9)


def test_blend_output_shape(identity_normalize):
    country = _weights_df([(1, 100_000, 100_000.0, 500, 2.0)])
    glob = _weights_df([(1, 1_000, 1_000.0, 5, 0.5)])
    out = blend_weights(country, glob, k=50_000)
    assert list(out.columns) == [
        "hour",
        "impressions",
        "adjusted_impressions",
        "clicks",
        "weight",
    ]


def test_normalize_weights_handles_zero_exposure():
    frame = pd.DataFrame(
        {"adjusted_impressions": [0.0], "unnormalized_weight": [1.5]}
    ).set_index(pd.Index([0], name="hour"))
    weights, factor = normalize_weights(frame)
    assert factor == 1.0
    assert weights.tolist() == [1.5]


def test_compute_all_countries_breaks_out_only_high_volume():
    # US clears MIN_COUNTRY_IMPRESSIONS, FJ does not, so only US gets its own set;
    # FJ's traffic still contributes to the pooled global set.
    hist = pd.concat(
        [
            _flat_hist("US", SWINGY_CTR, impressions=1_000_000),
            _flat_hist("FJ", SWINGY_CTR, impressions=1_000),
        ],
        ignore_index=True,
    )
    result = compute_all_countries(hist)

    assert list(result.columns) == [
        "hour",
        "impressions",
        "adjusted_impressions",
        "clicks",
        "weight",
    ] + ["country"]
    assert result["country"].isna().any()
    assert set(result.loc[result["country"].notna(), "country"]) == {"US"}
    # The global set pools both countries.
    global_rows = result[result["country"].isna()]
    assert global_rows["impressions"].sum() == 24 * 1_001_000
    # Every emitted weight must be finite and positive.
    assert np.isfinite(result["weight"]).all()
    assert (result["weight"] > 0).all()


def test_compute_all_countries_raises_without_a_global_set():
    # A snapshot with no usable global weights is a failure, not an empty table.
    with pytest.raises(ValueError):
        compute_all_countries(_flat_hist("GB", {hour: 0.0 for hour in range(24)}))
