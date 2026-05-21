"""Unit tests for statcounter_derived/pipeline.py."""

import hashlib
import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

PIPELINE_PATH = (
    Path(__file__).resolve().parents[4]
    / "sql"
    / "moz-fx-data-shared-prod"
    / "statcounter_derived"
    / "pipeline.py"
)

spec = importlib.util.spec_from_file_location("statcounter_pipeline", PIPELINE_PATH)
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)


# ---------- build_statcounter_url ----------


def test_build_statcounter_url_appends_with_existing_query():
    url = pipeline.build_statcounter_url(
        "https://example.com/path?bar=1", date(2026, 5, 1), date(2026, 5, 3)
    )
    assert url.startswith("https://example.com/path?bar=1&")
    assert "fromInt=20260501" in url
    assert "toInt=20260503" in url
    assert "fromMonthYear=2026-05" in url
    assert "fromDay=1" in url
    assert "toMonthYear=2026-05" in url
    assert "toDay=3" in url


def test_build_statcounter_url_appends_without_existing_query():
    url = pipeline.build_statcounter_url(
        "https://example.com/path", date(2026, 5, 1), date(2026, 5, 1)
    )
    assert "https://example.com/path?fromInt=20260501" in url


# # ---------- rename_columns ----------


def _csv_df(percent_col: str = "Market Share Perc. (1 May 2026)") -> pd.DataFrame:
    return pd.DataFrame({"Browser": ["Chrome", "Safari"], percent_col: [60.0, 20.0]})


def test_rename_columns_happy_path():
    df = pipeline.rename_columns(_csv_df(), date(2026, 5, 1))
    assert list(df.columns) == ["browser", "percent"]
    assert df["browser"].tolist() == ["Chrome", "Safari"]
    assert df["percent"].tolist() == [60.0, 20.0]


def test_rename_columns_missing_market_share_column():
    df = pd.DataFrame({"Browser": ["Chrome"], "Other": [1.0]})
    with pytest.raises(ValueError, match="Expected exactly one 'Market Share' column"):
        pipeline.rename_columns(df, date(2026, 5, 1))


def test_rename_columns_multiple_market_share_columns():
    df = pd.DataFrame(
        {
            "Browser": ["Chrome"],
            "Market Share Perc. (1 May 2026)": [60.0],
            "Market Share Perc. (2 May 2026)": [60.0],
        }
    )
    with pytest.raises(ValueError, match="Expected exactly one 'Market Share' column"):
        pipeline.rename_columns(df, date(2026, 5, 1))


def test_rename_columns_malformed_date_header():
    df = _csv_df(percent_col="Market Share Perc. (not a date)")
    with pytest.raises(ValueError, match="Could not parse date"):
        pipeline.rename_columns(df, date(2026, 5, 1))


def test_rename_columns_header_date_mismatch():
    df = _csv_df(percent_col="Market Share Perc. (2 May 2026)")
    with pytest.raises(ValueError, match="does not match partition_date"):
        pipeline.rename_columns(df, date(2026, 5, 1))


# # ---------- add_metadata ----------


def test_add_metadata_stamps_constant_values():
    df = pd.DataFrame({"browser": ["Chrome", "Safari"], "percent": [60.0, 20.0]})
    out = pipeline.add_metadata(df, date(2026, 5, 1), "Worldwide", "Desktop")
    assert out["date"].tolist() == ["2026-05-01", "2026-05-01"]
    assert out["geography"].tolist() == ["Worldwide", "Worldwide"]
    assert out["device"].tolist() == ["Desktop", "Desktop"]


# # ---------- add_surrogate_key ----------


def _stamped_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-05-01", "2026-05-01"],
            "geography": ["Worldwide", "Worldwide"],
            "device": ["Desktop", "Desktop"],
            "browser": ["Chrome", "Safari"],
            "percent": [60.0, 20.0],
        }
    )


def test_add_surrogate_key_matches_md5_of_pipe_joined_pk():
    out = pipeline.add_surrogate_key(_stamped_df())
    expected = hashlib.md5(b"2026-05-01|Worldwide|Desktop|Chrome").hexdigest()
    assert out.loc[0, "sk"] == expected


def test_add_surrogate_key_unique_across_rows():
    out = pipeline.add_surrogate_key(_stamped_df())
    assert out["sk"].nunique() == len(out)


def test_add_surrogate_key_changes_when_any_pk_col_changes():
    base = pipeline.add_surrogate_key(_stamped_df()).loc[0, "sk"]
    for col, new_value in [
        ("date", "2026-05-02"),
        ("geography", "Europe"),
        ("device", "Mobile"),
        ("browser", "Edge"),
    ]:
        df = _stamped_df()
        df.loc[0, col] = new_value
        assert pipeline.add_surrogate_key(df).loc[0, "sk"] != base


# # ---------- reorder_columns ----------


def test_reorder_columns_to_schema_order_and_drops_extras():
    df = _stamped_df().assign(sk="abc", extra="x")
    out = pipeline.reorder_columns(df)
    assert list(out.columns) == [
        "sk",
        "date",
        "geography",
        "device",
        "browser",
        "percent",
    ]


# # ---------- validate_no_nulls / validate_unique_pk / validate_pk ----------


def _pk_df() -> pd.DataFrame:
    return _stamped_df().assign(sk=["a", "b"])


def test_validate_no_nulls_clean():
    pipeline.validate_no_nulls(_pk_df())


def test_validate_no_nulls_raises_on_null():
    df = _pk_df()
    df.loc[0, "browser"] = None
    with pytest.raises(ValueError, match="Null value"):
        pipeline.validate_no_nulls(df)


def test_validate_no_nulls_raises_on_empty_string():
    df = _pk_df()
    df.loc[0, "browser"] = "   "
    with pytest.raises(ValueError, match="Empty value"):
        pipeline.validate_no_nulls(df)


def test_validate_unique_pk_passes_when_unique():
    pipeline.validate_unique_pk(_pk_df())


def test_validate_unique_pk_raises_on_duplicate():
    df = pd.concat([_pk_df().iloc[[0]], _pk_df().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate rows"):
        pipeline.validate_unique_pk(df)


def test_validate_pk_wrapper_runs_both_checks():
    pipeline.validate_pk(_pk_df())  # clean
    df_null = _pk_df()
    df_null.loc[0, "date"] = None
    with pytest.raises(ValueError, match="Null value"):
        pipeline.validate_pk(df_null)
    df_dup = pd.concat([_pk_df().iloc[[0]], _pk_df().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="Duplicate rows"):
        pipeline.validate_pk(df_dup)


# # ---------- validate_numeric_bounds ----------


def _bounds_df(percent) -> pd.DataFrame:
    return pd.DataFrame({"percent": percent})


def test_validate_numeric_bounds_valid():
    pipeline.validate_numeric_bounds(_bounds_df([0.0, 50.0, 100.0]))


def test_validate_numeric_bounds_raises_on_null():
    with pytest.raises(ValueError, match="Null value"):
        pipeline.validate_numeric_bounds(_bounds_df([50.0, None]))


def test_validate_numeric_bounds_raises_on_non_numeric():
    with pytest.raises(ValueError, match="Non-numeric"):
        pipeline.validate_numeric_bounds(_bounds_df([50.0, "oops"]))


def test_validate_numeric_bounds_raises_above_upper():
    with pytest.raises(ValueError, match="outside expected bounds"):
        pipeline.validate_numeric_bounds(_bounds_df([100.1]))


def test_validate_numeric_bounds_raises_below_lower():
    with pytest.raises(ValueError, match="outside expected bounds"):
        pipeline.validate_numeric_bounds(_bounds_df([-0.1]))


# # ---------- validate_min_row_count ----------


def test_validate_min_row_count_passes_with_rows():
    pipeline.validate_min_row_count(_pk_df())


def test_validate_min_row_count_raises_on_empty():
    with pytest.raises(ValueError, match="no rows"):
        pipeline.validate_min_row_count(_pk_df().iloc[0:0])


# # ---------- concatenate_dataframes ----------


def test_concatenate_dataframes_resets_index():
    a = _pk_df()
    b = _pk_df()
    out = pipeline.concatenate_dataframes([a, b])
    assert len(out) == len(a) + len(b)
    assert list(out.index) == list(range(len(out)))


# # ---------- build_sources ----------


def test_build_sources_cartesian_count():
    geos = [("Europe", "europe", "eu"), ("Asia", "asia", "as")]
    sources = pipeline.build_sources(geos)
    assert len(sources) == len(geos) * 2


def test_build_sources_devices_are_desktop_and_mobile():
    sources = pipeline.build_sources([("Europe", "europe", "eu")])
    assert {s.device for s in sources} == {"Desktop", "Mobile"}


def test_build_sources_url_encodes_geography_name():
    sources = pipeline.build_sources([("North America", "north-america", "na")])
    for s in sources:
        assert "region=North%20America" in s.base_url


def test_build_sources_substitutes_template_fields():
    sources = pipeline.build_sources([("Europe", "europe", "eu")])
    desktop = next(s for s in sources if s.device == "Desktop")
    mobile = next(s for s in sources if s.device == "Mobile")
    assert "/browser-market-share/desktop/europe" in desktop.base_url
    assert "device=Desktop" in desktop.base_url
    assert "device_hidden=desktop" in desktop.base_url
    assert "region_hidden=eu" in desktop.base_url
    assert "/browser-market-share/mobile/europe" in mobile.base_url
    assert "device=Mobile" in mobile.base_url
    assert "device_hidden=mobile" in mobile.base_url
