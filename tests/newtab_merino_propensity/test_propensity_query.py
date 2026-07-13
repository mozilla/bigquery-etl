"""Tests for newtab_merino_propensity_v2 query.py (multi-country weights)."""

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Import the query module from its file path.
_repo_root = Path(__file__).resolve().parent.parent.parent
_query_path = (
    _repo_root
    / "sql/moz-fx-data-shared-prod/telemetry_derived/newtab_merino_propensity_v2/query.py"
)

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not _query_path.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)

# Load the module dynamically since the path contains hyphens.
spec = importlib.util.spec_from_file_location(
    "newtab_merino_propensity_query", _query_path
)
assert spec is not None and spec.loader is not None
query_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(query_mod)

blend_weights = query_mod.blend_weights
compute_weights = query_mod.compute_weights
compute_all_countries = query_mod.compute_all_countries


def _weights_df(rows):
    """Build an output-shaped weights DataFrame from (position, fmt, imp, weight) rows."""
    return pd.DataFrame(
        [
            {
                "section_position": pd.NA,
                "position": pos,
                "tile_format": fmt,
                "impressions": imp,
                "weight": w,
            }
            for pos, fmt, imp, w in rows
        ]
    )


@pytest.fixture
def identity_normalize(monkeypatch):
    """Neuter the re-normalization so we can assert the raw shrinkage math."""
    monkeypatch.setattr(
        query_mod,
        "normalize_weights",
        lambda weights, hist: (weights["unnormalized_weight"], 1.0),
    )


def test_blend_high_volume_stays_country(identity_normalize):
    # imp_c >> K -> blended weight is essentially the country weight.
    country = _weights_df([(1, "medium-card", 10_000_000, 2.0)])
    glob = _weights_df([(1, "medium-card", 1_000, 0.5)])
    out = blend_weights(country, glob, hist_country=None, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx(2.0, rel=0.01)


def test_blend_low_volume_leans_global(identity_normalize):
    # imp_c -> 0 -> blended weight collapses to the global weight.
    country = _weights_df([(1, "medium-card", 0, 2.0)])
    glob = _weights_df([(1, "medium-card", 1_000, 0.5)])
    out = blend_weights(country, glob, hist_country=None, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx(0.5, rel=1e-6)


def test_blend_midpoint(identity_normalize):
    # imp_c == K -> exact 50/50 average of country and global weights.
    country = _weights_df([(1, "medium-card", 50_000, 2.0)])
    glob = _weights_df([(1, "medium-card", 1_000, 0.5)])
    out = blend_weights(country, glob, hist_country=None, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx((2.0 + 0.5) / 2, rel=1e-6)


def test_blend_missing_global_slot_falls_back_to_country(identity_normalize):
    # Slot present for the country but not global -> uses the country weight.
    country = _weights_df([(3, "large-card", 100_000, 1.7)])
    glob = _weights_df([(1, "medium-card", 1_000, 0.5)])
    out = blend_weights(country, glob, hist_country=None, k=50_000)
    assert out.loc[0, "weight"] == pytest.approx(1.7, rel=1e-6)


def test_blend_output_shape(identity_normalize):
    country = _weights_df([(1, "medium-card", 100_000, 2.0)])
    glob = _weights_df([(1, "medium-card", 1_000, 0.5)])
    out = blend_weights(country, glob, hist_country=None, k=50_000)
    assert list(out.columns) == [
        "section_position",
        "position",
        "tile_format",
        "impressions",
        "weight",
    ]


def test_compute_weights_empty_on_thin_input():
    # A tiny history cannot form reliable slots -> empty result, no exception.
    hist = pd.DataFrame(
        {
            "country": ["US"],
            "corpus_item_id": ["a"],
            "position": [1],
            "format": ["medium-card"],
            "impressions": [5],
            "clicks": [1],
        }
    )
    out = compute_weights(hist)
    assert out.empty


def _synthetic_hist(countries, items, positions, fmt="medium-card", imp=100, clicks=10):
    """Dense history: every (country, item, position) cell, enough to fit ALS."""
    records = []
    for country in countries:
        for item in items:
            for pos in positions:
                records.append(
                    {
                        "country": country,
                        "corpus_item_id": f"{country}-{item}",
                        "position": pos,
                        "format": fmt,
                        "impressions": imp,
                        "clicks": clicks,
                    }
                )
    return pd.DataFrame(records)


def test_compute_all_countries_smoke(monkeypatch):
    # Relax thresholds so small synthetic data fits, then exercise the full path:
    # global set (country IS NULL) + one blended set per country.
    monkeypatch.setattr(query_mod, "MIN_SLOT_ITEMS", 2)
    monkeypatch.setattr(query_mod, "MIN_SLOT_CLICKS", 1)
    monkeypatch.setattr(query_mod, "MIN_CELL_IMPRESSIONS", 1)
    monkeypatch.setattr(query_mod, "MIN_OUTPUT_IMPRESSIONS", 1)
    monkeypatch.setattr(query_mod, "MIN_COUNTRY_IMPRESSIONS", 1)

    hist = _synthetic_hist(
        countries=["US", "CA"],
        items=["a", "b", "c"],
        positions=[1, 2],
    )
    result = compute_all_countries(hist)

    assert not result.empty
    assert list(result.columns) == [
        "section_position",
        "position",
        "tile_format",
        "impressions",
        "weight",
    ] + ["country"]
    # Exactly one global (country IS NULL) set plus the two country sets.
    assert result["country"].isna().any()
    assert set(result.loc[result["country"].notna(), "country"]) == {"US", "CA"}
    # Every emitted weight must be finite and positive.
    assert np.isfinite(result["weight"]).all()
    assert (result["weight"] > 0).all()
