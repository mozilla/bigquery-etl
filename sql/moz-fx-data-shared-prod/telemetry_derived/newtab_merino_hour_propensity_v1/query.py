#!/usr/bin/env python3
"""Calculate time-of-day (UTC hour) propensity weights for newtab items.

Companion to ``newtab_merino_propensity_v2``, which removes position/format bias.
This job removes the remaining time-of-day bias, so consumers can compute a
doubly-adjusted exposure:

    aa_impressions = adjusted_impressions / hour_weight[country, hour]

The estimator is deliberately naive: the hour effect is a population aggregate
and is *not* adjusted for article quality. If curation systematically places
better or fresher articles at particular hours, that effect is absorbed into the
hour weight. The upgrade path -- should the diagnostics show it is needed -- is
to add hour as a third factor to the position job's ALS
(``log(ctr) = item_effect + slot_effect + hour_effect``) rather than to fit it
separately, because separate fits double-count correlated factors.

It is *not* naive with respect to position. The position weight already exists,
so it is applied first and the hour effect is estimated on position-adjusted
exposure. Otherwise a slate layout that varies by hour would leak position bias
into the hour weight.

Estimation, per country and UTC hour, over a whole number of weeks:

    ctr[c, h]  = SUM(clicks) / SUM(adjusted_impressions)
    global[c]  = SUM(clicks) / SUM(adjusted_impressions)   over all hours
    mult[c, h] = ctr[c, h] / global[c]
    unnormalized_weight[c, h] = 1.0 / mult[c, h]

which mirrors ``newtab_merino_propensity_v2``'s ``1.0 / mult`` so the consumption
convention (``exposure / weight``) is identical for both weights. Weights are
then scaled so that re-weighted exposure preserves total adjusted exposure over
the estimation window, and therefore preserves overall CTR.

Day-of-week is not a dimension; a multiple-of-7-days lookback averages over it
instead of modelling it. A lookback that is not a whole number of weeks would
bias particular hours.

Unlike the position job, country weights here are **not** shrunk toward the
global set, and the global set is **not** a fallback for a country that has
none. Position bias is a property of the slate, so every region shares it and
pooling is pure variance reduction. A UTC-hour curve is not shared: it is a
local diurnal pattern shifted by the region's offset from UTC, so the global
curve is a traffic-weighted mixture dominated by the largest markets. Blending
a region toward it does not reduce variance around the same quantity, it mixes
in a different one -- and for a region far enough from that mixture's phase the
correction would run the wrong way. No adjustment is the safe default, so
consumers COALESCE a missing country to 1.0 rather than to global.

The variance that shrinkage would have bought is not needed. Every country
clearing MIN_COUNTRY_IMPRESSIONS has at least a few thousand clicks per hour
cell over a two-week window (~1.6% relative error on the thinnest, against a
measured swing of 30-50%), so each country's own curve stands on its own.

The global (country IS NULL) row is still emitted: it is the correct weight for
exposure pooled over all countries, and it is the diagnostic baseline the
per-country curves are compared against. It is only wrong as a per-country
substitute.

Known limitation: a country spanning several timezones gets one UTC-hour curve
that is a within-country mixture of local curves, so its swing is damped and it
under-corrects at the edges. Keying on local hour would fix this -- the ping
carries ``metrics.quantity.newtab_content_utc_offset``, which the position job
already reads -- but UTC hour is what the offline panel validated, so that is a
v2 change rather than a silent divergence.
"""

import logging
from argparse import ArgumentParser
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Estimation window, in days ending at (and including) the snapshot date. Must be a
# multiple of 7: hour effects interact with day-of-week, so a partial week
# over-represents the hours of whichever weekdays it happens to include.
#
# Two weeks rather than four keeps this job's daily scan at parity with the position
# job it sits beside (1.90 TiB vs 1.64 TiB; four weeks measured 3.74 TiB). With only
# 24 cells, per-hour support is ample at two weeks, and the window adapts faster to
# genuine shifts. Raise via --lookback_days if the weights prove too noisy.
LOOKBACK_DAYS = 14

# The position weights are a daily snapshot too. Take the most recent one at or
# before the snapshot date, tolerating a few missed runs, and apply that single
# snapshot across the whole window -- which is how consumers apply it as well.
PROPENSITY_SNAPSHOT_LOOKBACK_DAYS = 14

# The layout the position weights were fit for; the only one currently emitted.
LAYOUT = "SECTION_GRID"

# A country needs at least this many impressions (over the window) to get its own
# emitted weight set. Below it the country is not broken out and gets no time-of-day
# adjustment at all -- see the module docstring on why falling back to the global
# curve would be worse than not adjusting. Same threshold as the position job; at
# this floor an hour cell still holds ~40k impressions.
MIN_COUNTRY_IMPRESSIONS = 1_000_000

# An (country, hour) cell below this many raw impressions is not emitted; consumers
# leave that cell unadjusted.
MIN_CELL_IMPRESSIONS = 2_000

# Hour cells below this many clicks are logged as thin support. At 1% relative error
# a cell needs ~10k clicks; this flags anything an order of magnitude below the
# support every currently-qualifying country has.
LOW_SUPPORT_CLICKS = 500

HOURS_PER_DAY = 24

HOURLY_EXPOSURE_SQL = """
WITH
pings AS (
  SELECT
    submission_timestamp,
    events,
    -- Resolve to a two-char country code (NULL when unresolvable), matching the
    -- normalized_country_code the downstream newtab_merino_extract job uses.
    mozfun.newtab.surface_id_country(
      metrics.string.newtab_content_surface_id,
      NULL,
      metrics.string.newtab_content_country
    ) AS country
  -- The stable table, not the live one the position job reads: this window is
  -- weeks long, and the stable table is already deduplicated by document_id.
  FROM `moz-fx-data-shared-prod.firefox_desktop.newtab_content`
  WHERE DATE(submission_timestamp) BETWEEN @window_start AND @date
    -- Keep all countries that resolve to a two-char code. Rows that resolve to
    -- NULL are dropped so every fetched row has a real country; the pooled
    -- "global" set (country IS NULL in the output) is built in Python.
    AND mozfun.newtab.surface_id_country(
      metrics.string.newtab_content_surface_id,
      NULL,
      metrics.string.newtab_content_country
    ) IS NOT NULL
),

flattened_newtab_events AS (
  SELECT
    p.country,
    -- Hour of the submission timestamp, matching how the offline panel that
    -- validated the hour effect bucketed its observations.
    EXTRACT(HOUR FROM p.submission_timestamp) AS hour,
    ue.name AS event_name,
    SAFE_CAST(mozfun.map.get_key(ue.extra, 'position') AS INT64) AS position,
    mozfun.map.get_key(ue.extra, 'format') AS format,
    SAFE_CAST(
      mozfun.map.get_key(ue.extra, 'section_position') AS INT64
    ) AS section_position
  FROM pings p
  CROSS JOIN UNNEST(p.events) AS ue
  WHERE ue.category IN ('pocket', 'newtab_content')
    AND ue.name IN ('impression', 'click')
    AND mozfun.map.get_key(ue.extra, 'corpus_item_id') IS NOT NULL
),

raw_grouped_totals AS (
  SELECT
    country,
    hour,
    position,
    format,
    section_position,
    SUM(CASE WHEN event_name = 'impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN event_name = 'click' THEN 1 ELSE 0 END) AS clicks
  FROM flattened_newtab_events
  GROUP BY
    country,
    hour,
    position,
    format,
    section_position
),

position_weights AS (
  SELECT
    country,
    SAFE_CAST(position AS INT64) AS position,
    tile_format,
    weight
  FROM `moz-fx-data-shared-prod.telemetry_derived.newtab_merino_propensity_v2`
  WHERE layout = @layout
    AND section_position IS NULL
    AND snapshot_date
      BETWEEN DATE_SUB(@date, INTERVAL @propensity_snapshot_lookback_days DAY)
      AND @date
  QUALIFY
    snapshot_date = MAX(snapshot_date) OVER ()
),

/* Section events carry position bias; divide it out first, exactly as the
   downstream extract job does (prefer exact-format weights, then 'any'). */
section_events AS (
  SELECT
    rw.country,
    rw.hour,
    rw.impressions,
    rw.clicks,
    rw.impressions / COALESCE(
      wt_country_exact.weight,
      wt_global_exact.weight,
      wt_country_any.weight,
      wt_global_any.weight,
      1.0
    ) AS adjusted_impressions
  FROM raw_grouped_totals rw
  LEFT JOIN position_weights wt_country_exact
    ON wt_country_exact.country = rw.country
    AND wt_country_exact.position = rw.position
    AND wt_country_exact.tile_format = rw.format
  LEFT JOIN position_weights wt_country_any
    ON wt_country_any.country = rw.country
    AND wt_country_any.position = rw.position
    AND wt_country_any.tile_format = 'any'
  LEFT JOIN position_weights wt_global_exact
    ON wt_global_exact.country IS NULL
    AND wt_global_exact.position = rw.position
    AND wt_global_exact.tile_format = rw.format
  LEFT JOIN position_weights wt_global_any
    ON wt_global_any.country IS NULL
    AND wt_global_any.position = rw.position
    AND wt_global_any.tile_format = 'any'
  WHERE rw.section_position IS NOT NULL
),

/* Position bias is a section-grid phenomenon, so non-section events pass through
   unadjusted -- again matching the extract job. Time-of-day bias is not
   section-specific, so both branches feed the hour estimate. */
non_section_events AS (
  SELECT
    country,
    hour,
    impressions,
    clicks,
    CAST(impressions AS FLOAT64) AS adjusted_impressions
  FROM raw_grouped_totals
  WHERE section_position IS NULL
),

combined_events AS (
  SELECT * FROM section_events
  UNION ALL
  SELECT * FROM non_section_events
)

SELECT
  country,
  hour,
  SUM(impressions) AS impressions,
  SUM(adjusted_impressions) AS adjusted_impressions,
  SUM(clicks) AS clicks
FROM combined_events
GROUP BY
  country,
  hour
"""

# Column contract for an emitted weight set.
WEIGHT_COLUMNS = ["hour", "impressions", "adjusted_impressions", "clicks", "weight"]
SUPPORT_COLUMNS = ["impressions", "adjusted_impressions", "clicks"]


def fetch_hourly_exposure(client, run_date, lookback_days):
    """Run the hourly exposure query and return a DataFrame.

    One row per (country, UTC hour) with raw impressions, position-adjusted
    impressions and clicks summed over the estimation window.
    """
    window_start = run_date - timedelta(days=lookback_days - 1)
    log.info(
        f"Fetching hourly exposure from BigQuery for {window_start} through "
        f"{run_date} ({lookback_days} days)..."
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", run_date),
            bigquery.ScalarQueryParameter("window_start", "DATE", window_start),
            bigquery.ScalarQueryParameter("layout", "STRING", LAYOUT),
            bigquery.ScalarQueryParameter(
                "propensity_snapshot_lookback_days",
                "INT64",
                PROPENSITY_SNAPSHOT_LOOKBACK_DAYS,
            ),
        ]
    )
    df = client.query(HOURLY_EXPOSURE_SQL, job_config=job_config).to_dataframe()
    log.info(f"Fetched {len(df):,} (country, hour) rows")
    return df


def normalize_weights(weights):
    """Scale unnormalized weights so adjusted_impressions/weight is conserved.

    ``weights`` is indexed by hour and carries ``adjusted_impressions`` and
    ``unnormalized_weight`` columns. The factor is chosen so that

        SUM(adjusted_impressions / weight) == SUM(adjusted_impressions)

    over the cells being emitted, which preserves overall CTR in the
    doubly-adjusted exposure space consumers will work in. (The position job
    conserves raw impressions for the same reason; here the quantity being divided
    is already position-adjusted, so that is what has to be conserved.)

    Returns the normalized weight Series aligned to ``weights.index`` and the scalar
    normalization factor.
    """
    exposure = weights["adjusted_impressions"].astype(float)
    reweighted = (exposure / weights["unnormalized_weight"]).sum()
    total = exposure.sum()

    if total <= 0 or not np.isfinite(reweighted):
        log.info("Cannot normalize (no adjusted exposure); leaving weights as-is.")
        return weights["unnormalized_weight"], 1.0

    normalization_factor = reweighted / total
    return weights["unnormalized_weight"] * normalization_factor, normalization_factor


def compute_weights(hist):
    """Compute hour propensity weights from a (country, hour) exposure frame.

    Operates on whatever exposure history it is handed (all countries pooled, or a
    single country's subset), collapsing it to one row per hour. Returns a DataFrame
    with columns hour, impressions, adjusted_impressions, clicks, weight -- or an
    empty DataFrame when the input has no usable hours.
    """
    hourly = hist.groupby("hour")[SUPPORT_COLUMNS].sum()

    thin = hourly[hourly["impressions"] < MIN_CELL_IMPRESSIONS]
    if len(thin) > 0:
        log.info(
            f"Dropping {len(thin)} hour(s) below {MIN_CELL_IMPRESSIONS:,} "
            f"impressions: {sorted(thin.index)}"
        )
    hourly = hourly[hourly["impressions"] >= MIN_CELL_IMPRESSIONS].copy()

    total_exposure = hourly["adjusted_impressions"].sum()
    total_clicks = hourly["clicks"].sum()
    if total_exposure <= 0 or total_clicks <= 0:
        log.info("No clicks or no adjusted exposure; returning no weights.")
        return pd.DataFrame(columns=WEIGHT_COLUMNS)

    # ctr[h] / global_ctr, i.e. how much more clickable this hour's traffic is.
    global_ctr = total_clicks / total_exposure
    ctr = hourly["clicks"] / hourly["adjusted_impressions"]
    mult = ctr / global_ctr

    hourly["unnormalized_weight"] = 1.0 / mult.where(mult > 0)

    usable = (
        hourly["unnormalized_weight"].notna()
        & np.isfinite(hourly["unnormalized_weight"])
        & (hourly["unnormalized_weight"] > 0)
    )
    if not usable.all():
        log.info(
            f"Dropping {(~usable).sum()} hour(s) with no usable weight: "
            f"{sorted(hourly.index[~usable])}"
        )
    hourly = hourly[usable]

    if len(hourly) == 0:
        log.info("No hours cleared the output filters; returning no weights.")
        return pd.DataFrame(columns=WEIGHT_COLUMNS)

    hourly["weight"], normalization_factor = normalize_weights(hourly)

    log.info(
        f"  all-hours ctr={global_ctr:.6f}, "
        f"normalization factor={normalization_factor:.6f}"
    )
    if len(hourly) < HOURS_PER_DAY:
        log.info(
            f"  only {len(hourly)} of {HOURS_PER_DAY} hours emitted; consumers leave "
            "the missing hours unadjusted."
        )

    return _as_output(hourly.reset_index())


def _as_output(frame):
    """Coerce a weights frame to the output column set and dtypes."""
    frame = frame.sort_values("hour").reset_index(drop=True)
    frame["hour"] = frame["hour"].astype(int)
    frame["impressions"] = frame["impressions"].astype(int)
    frame["adjusted_impressions"] = frame["adjusted_impressions"].astype(float)
    frame["clicks"] = frame["clicks"].astype(int)
    return frame[WEIGHT_COLUMNS]


def log_support(label, weights):
    """Log how well supported a weight set is, and warn on thin hour cells."""
    clicks = weights["clicks"]
    thin = weights.loc[clicks < LOW_SUPPORT_CLICKS, "hour"]
    # Relative standard error of an hour's CTR is about 1/sqrt(clicks).
    worst_error = 1.0 / np.sqrt(clicks.min()) if clicks.min() > 0 else float("nan")
    log.info(
        f"{label}: {len(weights)} hour(s), "
        f"min {int(clicks.min()):,} clicks/cell (~{worst_error:.1%} relative error), "
        f"weight range=[{weights['weight'].min():.4f}, {weights['weight'].max():.4f}]"
    )
    if len(thin) > 0:
        log.warning(
            f"{label}: {len(thin)} hour(s) below {LOW_SUPPORT_CLICKS:,} clicks: "
            f"{sorted(thin)}"
        )


def compute_all_countries(hist):
    """Compute the global hour weight set plus an independent set per high-volume country.

    The global set (all resolved countries pooled) is emitted with country = NULL. It
    is the right weight for exposure pooled across countries, and the baseline the
    per-country curves are read against -- it is NOT a substitute for a country that
    has no set of its own, because a UTC-hour curve is region-specific. See the module
    docstring.

    Every country whose total impressions clear MIN_COUNTRY_IMPRESSIONS gets its own
    set, estimated only from its own traffic with no shrinkage toward global. Returns
    a single DataFrame with a 'country' column added to the standard weight columns.
    """
    global_w = compute_weights(hist)
    if len(global_w) == 0:
        raise ValueError("No global hour weights could be computed for this snapshot.")
    log_support("global (all countries pooled)", global_w)
    global_w["country"] = None
    frames = [global_w]

    country_totals = (
        hist.groupby("country")["impressions"].sum().sort_values(ascending=False)
    )
    emitted = 0
    for country, total in country_totals.items():
        if total < MIN_COUNTRY_IMPRESSIONS:
            continue
        country_w = compute_weights(hist[hist["country"] == country])
        if len(country_w) == 0:
            log.info(
                f"{country}: {total:,} impressions but no fittable weights; skipping."
            )
            continue
        log_support(country, country_w)
        country_w["country"] = country
        frames.append(country_w)
        emitted += 1

    result = pd.concat(frames, ignore_index=True)
    log.info(f"Emitted global + {emitted} country sets; {len(result)} total rows")
    return result


def main():
    """Entry point."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination_dataset", default="telemetry_derived")
    parser.add_argument(
        "--destination_table", default="newtab_merino_hour_propensity_v1"
    )
    parser.add_argument(
        "--lookback_days",
        type=int,
        default=LOOKBACK_DAYS,
        help=(
            "Estimation window length in days, ending at --date. Must be a multiple "
            "of 7 so every hour sees the same mix of weekdays."
        ),
    )
    args = parser.parse_args()

    if args.lookback_days % 7 != 0:
        parser.error(
            f"--lookback_days must be a multiple of 7, got {args.lookback_days}"
        )

    client = bigquery.Client(args.project)

    hist = fetch_hourly_exposure(client, args.date, args.lookback_days)
    result = compute_all_countries(hist)
    result["snapshot_date"] = args.date
    result["snapshot_at"] = datetime.now(timezone.utc)
    result = result[
        [
            "snapshot_date",
            "snapshot_at",
            "country",
            "hour",
            "impressions",
            "adjusted_impressions",
            "clicks",
            "weight",
        ]
    ]

    destination = (
        f"{args.project}.{args.destination_dataset}."
        f"{args.destination_table}${args.date:%Y%m%d}"
    )
    log.info(f"Writing {len(result)} rows to {destination} (WRITE_TRUNCATE)")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(result, destination, job_config=job_config)
    job.result()

    log.info("Done.")


if __name__ == "__main__":
    main()
