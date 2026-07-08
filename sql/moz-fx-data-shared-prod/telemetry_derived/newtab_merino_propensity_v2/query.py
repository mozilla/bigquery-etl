#!/usr/bin/env python3
"""Calculate propensity weights for newtab items using lattice decomposition.

Uses Alternating Least Squares (ALS) to separate position/format effects from item
quality, producing normalized weights that preserve overall CTR.

This is solving a matrix factorization problem. The core idea:

  Model: log(ctr) = item_effect + slot_effect

  Every observed CTR for an (item, position, format) cell is modeled as the sum of two independent effects:
  - item_effect -- how inherently clickable the item is (quality, topic, headline)
  - slot_effect -- how much the position/format boosts or suppresses clicks (position bias)

  Working in log space makes this additive; in real space it's multiplicative: ctr = item_quality × slot_multiplier.

  Why ALS? You can't solve for both effects simultaneously, so you alternate:

  1. Fix slot effects, solve for item effects: For each item, average log_ctr - slot_effect across all positions that item appeared in (weighted
  by impressions). This gives each item's intrinsic quality, after removing the position bias.
  2. Fix item effects, solve for slot effects: For each (position, format) slot, average log_ctr - item_effect across all items that appeared
  there (weighted by impressions). This gives each slot's position bias, after removing item quality.
  3. Repeat. Each iteration refines both estimates. max_change tracks convergence -- when slot effects stop moving, the decomposition has
  stabilized.

  The previous SQL approach estimated position bias from "fresh items" assumed to be randomly ranked, but
  this is no longer the case with inferred personalization that pre-biases fresh items.
"""

import logging
from argparse import ArgumentParser
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PER_ITEM_CUTOFF = 20  # After 20, we compute format independent of position
MAX_POSITION = 200
MIN_SLOT_ITEMS = 200
MIN_SLOT_CLICKS = 100
MIN_CELL_IMPRESSIONS = 10
MIN_OUTPUT_IMPRESSIONS = 2000
ALS_ITERATIONS = 30
LAYOUT = "SECTION_GRID"

# A country needs at least this many impressions (over the window) to get its own
# emitted propensity set. Below it, the country is not broken out and consumers fall
# back to the global (country IS NULL) set.
MIN_COUNTRY_IMPRESSIONS = 1_000_000
# Shrinkage strength (in impression units) for blending a country's per-slot weight
# toward the global weight: weight = (imp_c * w_c + K * w_global) / (imp_c + K).
# High-volume slots stay country-specific; sparse slots lean global.
BLEND_PSEUDOCOUNT = 50_000

HISTORICAL_IMPRESSIONS_SQL = """
WITH
private_pings AS (
  SELECT
    DATE(submission_timestamp) AS date,
    submission_timestamp,
    document_id,
    events,
    -- Resolve to a two-char country code (NULL when unresolvable), matching the
    -- normalized_country_code the downstream newtab_merino_extract job uses.
    mozfun.newtab.surface_id_country(
      metrics.string.newtab_content_surface_id,
      NULL,
      metrics.string.newtab_content_country
    ) AS country,
    metrics.object.newtab_content_inferred_interests AS inferred_interests,
    CAST(metrics.quantity.newtab_content_utc_offset AS NUMERIC) AS raw_offset
  FROM `moz-fx-data-shared-prod.firefox_desktop.newtab_content_live`
  -- The original job uses CURRENT_TIMESTAMP() - 10 days. This partitioned job
  -- takes a DATE at midnight and caps the end at the next midnight, so it spans
  -- 11 calendar dates: @date - 10 through @date.
  WHERE submission_timestamp >= TIMESTAMP_SUB(TIMESTAMP(@date), INTERVAL 10 DAY)
    AND submission_timestamp < TIMESTAMP_ADD(TIMESTAMP(@date), INTERVAL 1 DAY)
    -- Keep all countries that resolve to a two-char code. Rows that resolve to
    -- NULL are dropped so every fetched row has a real country; the pooled
    -- "global" set (country IS NULL in the output) is built in Python.
    AND mozfun.newtab.surface_id_country(
      metrics.string.newtab_content_surface_id,
      NULL,
      metrics.string.newtab_content_country
    ) IS NOT NULL
),

deduplicated_pings AS (
  SELECT *
  FROM private_pings
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DATE(submission_timestamp), document_id
    ORDER BY submission_timestamp DESC
  ) = 1
),

flattened_newtab_events AS (
  SELECT
    dp.country,
    ue.name AS event_name,
    mozfun.map.get_key(ue.extra, 'corpus_item_id') AS corpus_item_id,
    mozfun.map.get_key(ue.extra, 'position') AS position,
    mozfun.map.get_key(ue.extra, 'format') AS format,
    mozfun.map.get_key(ue.extra, 'section_position') AS section_position
  FROM deduplicated_pings dp
  CROSS JOIN UNNEST(dp.events) AS ue
  WHERE ue.category IN ('pocket', 'newtab_content')
    AND ue.name IN ('impression', 'click')
    AND mozfun.map.get_key(ue.extra, 'corpus_item_id') IS NOT NULL
),

raw_grouped_totals AS (
  SELECT
    fne.country,
    fne.corpus_item_id,
    SAFE_CAST(fne.position AS INT64) AS position,
    fne.format,
    SUM(CASE WHEN fne.event_name = 'impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN fne.event_name = 'click' THEN 1 ELSE 0 END) AS clicks
  FROM flattened_newtab_events fne
  WHERE SAFE_CAST(fne.position AS INT64) <= 200
    AND fne.section_position IS NOT NULL
  GROUP BY
    fne.country,
    fne.corpus_item_id,
    position,
    fne.format
)
SELECT
  rw.country,
  rw.corpus_item_id,
  rw.impressions,
  rw.clicks,
  rw.format,
  rw.position,
  SAFE_DIVIDE(rw.clicks, rw.impressions) AS ctr
FROM raw_grouped_totals rw
"""


def fetch_historical_impressions(client, run_date):
    """Run the historical impressions query and return a DataFrame."""
    log.info(f"Fetching historical impressions from BigQuery for {run_date}...")
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("date", "DATE", run_date),
        ]
    )
    df = client.query(HISTORICAL_IMPRESSIONS_SQL, job_config=job_config).to_dataframe()
    log.info(f"Fetched {len(df):,} rows")
    return df


EMPTY_WEIGHTS = ["section_position", "position", "tile_format", "impressions", "weight"]


def normalize_weights(weights, hist):
    """Scale unnormalized weights so impressions/weight preserves overall CTR.

    ``weights`` is indexed by (position, format) with an ``unnormalized_weight``
    column; ``hist`` is the impression history the weights were derived from. The
    factor is chosen so the total re-weighted impressions over the matched slots is
    about the same as the raw impressions (i.e. overall CTR is preserved).

    Returns the normalized weight Series aligned to ``weights.index`` and the scalar
    normalization factor.
    """
    all_agg = (
        hist[hist["position"] <= MAX_POSITION]
        .groupby(["position", "format"])[["impressions", "clicks"]]
        .sum()
    )
    norm_data = all_agg.join(weights[["unnormalized_weight"]], how="inner")

    denom_sum = (norm_data["impressions"] / norm_data["unnormalized_weight"]).sum()
    total_clicks = all_agg["clicks"].sum()
    target_ctr = total_clicks / all_agg["impressions"].sum()

    normalization_factor = target_ctr * denom_sum / norm_data["clicks"].sum()
    return weights["unnormalized_weight"] * normalization_factor, normalization_factor


def compute_weights(hist):
    """Compute propensity weights using lattice decomposition with ALS.

    Operates on whatever impression history it is handed (all countries pooled, or a
    single country's subset). Returns a DataFrame with columns: section_position,
    position, tile_format, impressions, weight. Returns an empty DataFrame when the
    input is too thin to fit the decomposition (e.g. a small country with no reliable
    slots).
    """
    # === Phase 1: Build the lattice ===
    cell = (
        hist[hist["position"] <= MAX_POSITION]
        .groupby(["corpus_item_id", "position", "format"])[["impressions", "clicks"]]
        .sum()
    )
    cell = cell[cell["impressions"] >= MIN_CELL_IMPRESSIONS].copy()
    cell["ctr"] = cell["clicks"] / cell["impressions"]

    # Only items appearing at 2+ slots (bridge items)
    item_slot_count = cell.reset_index().groupby("corpus_item_id").size()
    bridge_items = item_slot_count[item_slot_count >= 2].index
    cell_bridge = cell.loc[
        cell.index.get_level_values("corpus_item_id").isin(bridge_items)
    ]

    # Identify reliable slots
    slot_stats = (
        cell_bridge.reset_index()
        .groupby(["position", "format"])
        .agg(
            n_items=("corpus_item_id", "nunique"),
            total_clicks=("clicks", "sum"),
        )
    )
    reliable_slots = set(
        slot_stats[
            (slot_stats["n_items"] >= MIN_SLOT_ITEMS)
            & (slot_stats["total_clicks"] >= MIN_SLOT_CLICKS)
        ].index
    )

    # Filter to reliable slots and bridge items
    cell_fit = cell_bridge.reset_index()
    cell_fit = cell_fit[
        pd.MultiIndex.from_arrays([cell_fit["position"], cell_fit["format"]]).isin(
            reliable_slots
        )
    ].copy()

    # Log-space decomposition requires positive CTR
    cell_fit = cell_fit[cell_fit["ctr"] > 0].copy()
    cell_fit["log_ctr"] = np.log(cell_fit["ctr"])

    log.info(
        f"Lattice: {len(reliable_slots)} reliable slots, "
        f"{len(bridge_items):,} bridge items, {len(cell_fit):,} cells"
    )

    if len(cell_fit) == 0 or len(reliable_slots) == 0:
        log.info("Not enough data to fit the decomposition; returning no weights.")
        return pd.DataFrame(columns=EMPTY_WEIGHTS)

    # === Phase 2: Alternating least squares in log space ===
    # Model: log(ctr) = item_effect + slot_effect
    # Vectorized with numpy integer indexing and bincount.
    item_ids = pd.Categorical(cell_fit["corpus_item_id"])
    slot_keys = list(zip(cell_fit["position"], cell_fit["format"]))
    slot_cat = pd.Categorical(slot_keys)
    item_idx = item_ids.codes
    slot_idx = slot_cat.codes
    n_items = len(item_ids.categories)
    n_slots = len(slot_cat.categories)
    log_ctr = cell_fit["log_ctr"].values
    weights = cell_fit["impressions"].values.astype(float)

    item_eff = np.zeros(n_items)
    slot_eff = np.zeros(n_slots)

    for iteration in range(ALS_ITERATIONS):
        # Item effects: weighted mean of (log_ctr - slot_eff) per item
        residual = (log_ctr - slot_eff[slot_idx]) * weights
        item_eff = np.bincount(
            item_idx, weights=residual, minlength=n_items
        ) / np.bincount(item_idx, weights=weights, minlength=n_items)

        # Slot effects: weighted mean of (log_ctr - item_eff) per slot
        residual = (log_ctr - item_eff[item_idx]) * weights
        new_slot_eff = np.bincount(
            slot_idx, weights=residual, minlength=n_slots
        ) / np.bincount(slot_idx, weights=weights, minlength=n_slots)

        max_change = np.max(np.abs(new_slot_eff - slot_eff))
        slot_eff = new_slot_eff

        if iteration < 2 or iteration == ALS_ITERATIONS - 1:
            log.info(f"  ALS iter {iteration}: max_change={max_change:.10f}")

    # Convert log slot effects to multipliers, keyed by (position, format)
    slot_multiplier = {
        slot_cat.categories[i]: np.exp(slot_eff[i]) for i in range(n_slots)
    }

    # === Phase 3: Convert to weights and handle long-tail ===
    per_pos_data = []
    for (pos, fmt), mult in slot_multiplier.items():
        if pos <= PER_ITEM_CUTOFF:
            agg = hist[(hist["position"] == pos) & (hist["format"] == fmt)]
            total_imp = agg["impressions"].sum()
            per_pos_data.append(
                {
                    "position": pos,
                    "format": fmt,
                    "multiplier": mult,
                    "unnormalized_weight": 1.0 / mult if mult > 0 else np.nan,
                    "impressions": total_imp,
                }
            )

    per_pos_df = pd.DataFrame(per_pos_data).set_index(["position", "format"])

    # Long-tail: pool slots > PER_ITEM_CUTOFF by format
    lt_slots = {k: v for k, v in slot_multiplier.items() if k[0] > PER_ITEM_CUTOFF}
    lt_by_fmt = {}
    for fmt in cell_fit["format"].unique():
        fmt_slots = {k: v for k, v in lt_slots.items() if k[1] == fmt}
        if fmt_slots:
            slot_imps = []
            for (p, f), m in fmt_slots.items():
                imp = hist[(hist["position"] == p) & (hist["format"] == f)][
                    "impressions"
                ].sum()
                slot_imps.append((m, imp))
            total_imp = sum(i for _, i in slot_imps)
            if total_imp > 0:
                avg_mult = sum(m * i for m, i in slot_imps) / total_imp
                lt_by_fmt[fmt] = {
                    "multiplier": avg_mult,
                    "unnormalized_weight": 1.0 / avg_mult if avg_mult > 0 else np.nan,
                    "impressions": total_imp,
                }

    lt_rows = []
    for pos in range(PER_ITEM_CUTOFF + 1, MAX_POSITION + 1):
        for fmt, data in lt_by_fmt.items():
            lt_rows.append(
                {
                    "position": pos,
                    "format": fmt,
                    "multiplier": data["multiplier"],
                    "unnormalized_weight": data["unnormalized_weight"],
                    "impressions": data["impressions"],
                }
            )
    lt_df = pd.DataFrame(lt_rows)
    if len(lt_df) > 0:
        lt_df = lt_df.set_index(["position", "format"])

    merged = pd.concat([per_pos_df, lt_df])

    # Filter: valid weight and minimum impressions
    merged = merged[
        (merged["impressions"] > MIN_OUTPUT_IMPRESSIONS)
        & merged["unnormalized_weight"].notna()
        & np.isfinite(merged["unnormalized_weight"])
        & (merged["unnormalized_weight"] > 0)
    ]

    if len(merged) == 0:
        log.info("No slots cleared the output filters; returning no weights.")
        return pd.DataFrame(columns=EMPTY_WEIGHTS)

    # === Phase 4: Normalize weights ===
    merged["weight"], normalization_factor = normalize_weights(merged, hist)

    log.info(f"Normalization: factor={normalization_factor:.4f}")

    # === Phase 5: Build output with 'any' format ===
    output = merged[["weight", "impressions"]].reset_index()

    # 'any' format: impression-weighted average weight per position
    actual_imp = (
        hist[hist["position"] <= MAX_POSITION]
        .groupby(["position", "format"])["impressions"]
        .sum()
    )

    any_list = []
    for pos in output["position"].unique():
        pos_rows = output[output["position"] == pos]
        weighted_sum = 0
        imp_sum = 0
        for _, r in pos_rows.iterrows():
            real_imp = actual_imp.get((int(r["position"]), r["format"]), 0)
            # Fall back to output row impressions when no actual per-position data
            imp = real_imp if real_imp > 0 else r["impressions"]
            weighted_sum += r["weight"] * imp
            imp_sum += imp
        if imp_sum > 0:
            any_list.append(
                {
                    "position": pos,
                    "format": "any",
                    "weight": weighted_sum / imp_sum,
                    "impressions": imp_sum,
                }
            )
    any_rows = pd.DataFrame(any_list)

    output = pd.concat([output, any_rows], ignore_index=True)
    output = output.sort_values(["position", "format"]).reset_index(drop=True)

    # Rename and add columns to match schema
    output = output.rename(columns={"format": "tile_format"})
    output["section_position"] = pd.array([None] * len(output), dtype=pd.Int64Dtype())
    output["position"] = output["position"].astype(int)
    output["impressions"] = output["impressions"].astype(int)

    log.info(
        f"Output: {len(output)} rows, "
        f"{output['position'].nunique()} positions, "
        f"formats={sorted(output['tile_format'].unique())}"
    )

    return output[EMPTY_WEIGHTS]


def blend_weights(country_w, global_w, hist_country, k=BLEND_PSEUDOCOUNT):
    """Blend a country's weights toward the global weights via shrinkage.

    Per (position, tile_format) slot, with imp_c the country's impressions for that
    slot:

        weight = (imp_c * w_country + k * w_global) / (imp_c + k)

    High-volume slots stay close to the country's own weight; sparse slots lean
    toward global. Slots the global set lacks fall back to the country's weight. The
    blended set is re-normalized against the country's own history so that
    impressions/weight preserves the country's overall CTR (as compute_weights does).

    Only slots present for the country are emitted; slots the country never served
    are left to the global (country IS NULL) fallback downstream.

    Returns a DataFrame with columns: section_position, position, tile_format,
    impressions, weight.
    """
    keys = ["position", "tile_format"]
    merged = country_w.merge(
        global_w[keys + ["weight"]], on=keys, how="left", suffixes=("_c", "_g")
    )
    imp_c = merged["impressions"].astype(float)
    w_c = merged["weight_c"]
    w_g = merged["weight_g"].where(merged["weight_g"].notna(), w_c)
    merged["unnormalized_weight"] = (imp_c * w_c + k * w_g) / (imp_c + k)

    # Re-normalize against the country's own history (normalize_weights groups the
    # history on the 'format' column and matches weights by (position, format)).
    norm_input = merged.rename(columns={"tile_format": "format"}).set_index(
        ["position", "format"]
    )
    _, normalization_factor = normalize_weights(norm_input, hist_country)
    merged["weight"] = merged["unnormalized_weight"] * normalization_factor

    merged["section_position"] = pd.array([None] * len(merged), dtype=pd.Int64Dtype())
    merged["position"] = merged["position"].astype(int)
    merged["impressions"] = merged["impressions"].astype(int)
    return merged[EMPTY_WEIGHTS]


def compute_all_countries(hist):
    """Compute the global propensity set plus a blended set per high-volume country.

    The global set (all resolved countries pooled) is emitted with country = NULL.
    Every country whose total impressions clear MIN_COUNTRY_IMPRESSIONS gets its own
    set, blended toward global by impression volume. Returns a single DataFrame with a
    'country' column added to the standard weight columns.
    """
    global_w = compute_weights(hist)
    global_w["country"] = None
    frames = [global_w]

    country_totals = (
        hist.groupby("country")["impressions"].sum().sort_values(ascending=False)
    )
    emitted = 0
    for country, total in country_totals.items():
        if total < MIN_COUNTRY_IMPRESSIONS:
            continue
        hist_country = hist[hist["country"] == country]
        country_w = compute_weights(hist_country)
        if len(country_w) == 0:
            log.info(
                f"{country}: {total:,} impressions but no fittable weights; skipping."
            )
            continue
        blended = blend_weights(country_w, global_w, hist_country)
        blended["country"] = country
        frames.append(blended)
        emitted += 1
        log.info(f"{country}: {total:,} impressions -> {len(blended)} blended slots")

    result = pd.concat(frames, ignore_index=True)
    log.info(f"Emitted global + {emitted} country sets; {len(result)} total rows")
    return result


def main():
    """Entry point."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination_dataset", default="telemetry_derived")
    parser.add_argument("--destination_table", default="newtab_merino_propensity_v2")
    args = parser.parse_args()

    client = bigquery.Client(args.project)

    hist = fetch_historical_impressions(client, args.date)
    result = compute_all_countries(hist)
    result["snapshot_date"] = args.date
    result["snapshot_at"] = datetime.now(timezone.utc)
    result["layout"] = LAYOUT
    result = result[
        [
            "snapshot_date",
            "snapshot_at",
            "layout",
            "country",
            "section_position",
            "position",
            "tile_format",
            "impressions",
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
