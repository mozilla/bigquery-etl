"""SECTION 1: METRIC DEFINITIONS.

The Firefox Desktop production default guardrail set, hard-coded. In the real system these come
from metric-hub through metric-config-parser; here they are literal so the proof of concept has no
cross-repo dependency. The shapes below are what the declarative format has to be able to express,
so this file doubles as the target for that work.

Each metric carries three things: which source table it reads, how to reduce a unit's rows inside a
window to one number, and which windows it declares.
"""

from dataclasses import dataclass
from typing import Callable

# Window rules are a KIND and a LENGTH, never enumerated bounds. Enumerated bounds terminate the
# series: four weekly windows go quiet after week four even though the experiment runs for months.
# A rule extends for as long as the experiment does. Length 7 for both families keeps a young
# experiment producing something in its first week while still extending indefinitely.
CUMULATIVE_WEEKLY = {"kind": "cumulative", "length": 7}
DISJOINT_WEEKLY = {"kind": "disjoint", "length": 7}

# Retention is a 0/1 "was the unit active in this window", so a cumulative window is ~1.0 by
# construction and says nothing. Those metrics take disjoint weeks instead.
# `is_pinned` and `is_default_browser` are also 0/1 and also monotone over a cumulative window, so
# the same argument arguably applies to them. They are left cumulative to match the desktop
# guardrail set being ported; whether they should move is a question for the metric definitions.
RETENTION_METRICS = {"retained", "retained_dau", "active_in_last_3_days_legacy"}


# ------------------------------------------------------------------- per-unit reducers ----
# A reducer is how one unit's rows become one number, split into composable parts because the
# aggregation reduces a unit's rows once per disjoint bucket and then composes buckets into
# windows:
#
#   bucket_agg   reduces the unit's rows inside ONE bucket to a raw number
#   combine      how those raw numbers compose over the buckets a window spans, SUM or MIN
#   no_rows      the raw value a unit with no rows in the window takes
#   finalize     turns the combined raw number into the metric's value
#
# The split exists to keep the 0/1 metrics correct. A threshold must be applied ONCE, to the
# combined raw value, never per bucket: `retained` carries a SUM per bucket and tests `> 0` at the
# end, because per-bucket thresholds combined afterwards answer a different question as soon as the
# column can be negative or the combination is anything but an OR.


def _identity(combined):
    return combined


def _above_zero(combined):
    return f"CAST({combined} > 0 AS INT64)"


@dataclass(frozen=True)
class Reducer:
    """How one unit's rows in one window become one number, in bucket-composable parts."""

    bucket_agg: str
    combine: str = "SUM"
    no_rows: str = "0"
    finalize: Callable[[str], str] = _identity


def per_unit_distinct_days():
    """Distinct days the unit appears inside the window.

    Safe to SUM across buckets only because buckets partition tenure: a submission_date falls in
    exactly one bucket, so per-bucket distinct counts add up to the window's distinct count with no
    double counting. A combiner that summed overlapping ranges would be wrong here.
    """
    return Reducer(bucket_agg="COUNT(DISTINCT submission_date)")


def per_unit_sum(column):
    """Sum of `column` across the unit's rows inside the window."""
    return Reducer(bucket_agg=f"SUM({column})")


def per_unit_any(condition):
    """1 if `condition` held on any of the unit's rows inside the window, else 0.

    Carries the COUNTIF per bucket and tests it once at the end, so the bucket grid never sees the
    threshold. This 0/1 reduction is what lets binary metrics travel the same sufficient-statistics
    path as the continuous ones, as a linear probability model.
    """
    return Reducer(bucket_agg=f"COUNTIF({condition})", finalize=_above_zero)


def per_unit_countif(condition):
    """Count the unit's rows inside the window on which `condition` held."""
    return Reducer(bucket_agg=f"COUNTIF({condition})")


def per_unit_sum_positive(column):
    """1 if the unit's summed `column` over the window is positive, else 0.

    The raw SUM is what travels, and `> 0` is applied to the summed total. Testing each bucket and
    combining the answers is a different metric: it asks whether any single bucket was positive,
    which diverges from the window total the moment `column` can be negative.

    `no_rows` keeps metric-hub's 0-fill. SUM returns NULL when a unit has no rows in the window,
    and that NULL would otherwise make the unit's value NULL rather than 0, which the rollup drops
    from the numerator while still counting the unit in `n`.
    """
    return Reducer(bucket_agg=f"SUM({column})", finalize=_above_zero)


def per_unit_min_below(expression, threshold, no_rows_value):
    """1 if the minimum of `expression` over the unit's rows in the window is below `threshold`.

    Combines with MIN rather than SUM, since the minimum over a window is the minimum of its
    buckets' minimums. `no_rows_value` is the sentinel a unit with no rows in the window gets,
    applied after the combination for the same reason the thresholds are.
    """
    return Reducer(
        bucket_agg=f"MIN({expression})",
        combine="MIN",
        no_rows=str(no_rows_value),
        finalize=lambda combined: f"CAST({combined} < {threshold} AS INT64)",
    )


def per_unit_scaled(reducer, factor):
    """Multiply `reducer` by a constant.

    The constant cancels in everything reported here: it divides out of a relative difference, and
    out of theta, where it scales numerator and denominator alike. So a scaled metric and its
    unscaled twin produce identical intervals, differing only in the raw sums. Both are kept because
    both are in the set being ported, and production tells them apart by reading each with a
    different statistic; under one uniform method that distinction disappears.
    """
    return Reducer(
        bucket_agg=reducer.bucket_agg,
        combine=reducer.combine,
        no_rows=reducer.no_rows,
        finalize=lambda combined: f"({reducer.finalize(combined)}) * {factor}",
    )


# ------------------------------------------------------------------------ definitions ----


@dataclass(frozen=True)
class Metric:
    """One metric: where it reads from, how it reduces a unit's rows, and its windows."""

    name: str
    source: str
    reducer: Reducer
    window_rules: tuple = (CUMULATIVE_WEEKLY,)


def _metric(name, source, reducer):
    rules = (DISJOINT_WEEKLY,) if name in RETENTION_METRICS else (CUMULATIVE_WEEKLY,)
    return Metric(name=name, source=source, reducer=reducer, window_rules=rules)


CLIENTS_DAILY = "clients_daily"
SEARCH = "search"
ACTIVE_USERS = "active_users"

GUARDRAILS = [
    _metric("days_of_use", CLIENTS_DAILY, per_unit_distinct_days()),
    _metric(
        "qualified_cumulative_days_of_use",
        CLIENTS_DAILY,
        per_unit_countif(
            "active_hours_sum > 0 AND "
            "scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum > 0"
        ),
    ),
    _metric("active_hours", CLIENTS_DAILY, per_unit_sum("active_hours_sum")),
    _metric(
        "uri_count",
        CLIENTS_DAILY,
        per_unit_sum("scalar_parent_browser_engagement_total_uri_count_sum"),
    ),
    _metric(
        "is_pinned",
        CLIENTS_DAILY,
        per_unit_any("scalar_parent_os_environment_is_taskbar_pinned"),
    ),
    _metric("is_default_browser", CLIENTS_DAILY, per_unit_any("is_default_browser")),
    _metric(
        "retained", CLIENTS_DAILY, per_unit_sum_positive("pings_aggregated_by_this_row")
    ),
    _metric("search_count", SEARCH, per_unit_sum("sap")),
    _metric("ad_clicks", SEARCH, per_unit_sum("ad_click")),
    _metric("searches_with_ads", SEARCH, per_unit_sum("search_with_ads")),
    _metric("organic_search_count", SEARCH, per_unit_sum("organic")),
    _metric("tagged_search_count", SEARCH, per_unit_sum("tagged_sap")),
    _metric("tagged_follow_on_search_count", SEARCH, per_unit_sum("tagged_follow_on")),
    _metric("retained_dau", ACTIVE_USERS, per_unit_any("is_dau")),
    _metric(
        "active_in_last_3_days_legacy",
        ACTIVE_USERS,
        per_unit_min_below("mozfun.bits28.days_since_seen(days_active_bits)", 3, 30),
    ),
    _metric(
        "client_level_daily_active_users_v2", ACTIVE_USERS, per_unit_countif("is_dau")
    ),
    _metric(
        "daily_active_users_per_1000_clients_legacy",
        ACTIVE_USERS,
        per_unit_scaled(per_unit_countif("is_dau"), 1000),
    ),
]

# One entry per source table: where to read it, which columns the metrics above need, and any
# restriction the metric-hub data source carries. Keeping the restriction in the source CTE's WHERE
# is the same filter in one scan.
SOURCES = {
    CLIENTS_DAILY: dict(
        table="moz-fx-data-shared-prod.telemetry.clients_daily",
        unit_column="client_id",
        columns=[
            "active_hours_sum",
            "scalar_parent_browser_engagement_total_uri_count_sum",
            "scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum",
            "scalar_parent_os_environment_is_taskbar_pinned",
            "is_default_browser",
            "pings_aggregated_by_this_row",
        ],
    ),
    SEARCH: dict(
        table="moz-fx-data-shared-prod.search.search_clients_engines_sources_daily",
        unit_column="client_id",
        columns=[
            "sap",
            "ad_click",
            "search_with_ads",
            "organic",
            "tagged_sap",
            "tagged_follow_on",
        ],
    ),
    # metric-hub's firefox_desktop_active_users_view is
    # (SELECT * FROM ...telemetry.desktop_active_users WHERE is_desktop).
    ACTIVE_USERS: dict(
        table="moz-fx-data-shared-prod.telemetry.desktop_active_users",
        unit_column="client_id",
        where="is_desktop",
        columns=["is_dau", "days_active_bits"],
    ),
}


def metric_definitions():
    """Group the run's metric set by the source table each one reads.

    Grouped because one query per source is what makes adding a metric to an
    already-scanned table nearly free, which is the property the cost model rests on.
    """
    by_source = {}
    for metric in GUARDRAILS:
        by_source.setdefault(metric.source, []).append(metric)
    return by_source
