#!/usr/bin/env python3
"""Calculate propensity weights for newtab items using lattice decomposition.

Uses alternating least squares to separate position/format effects from item
quality, producing normalized weights that preserve overall CTR.

This is solving a matrix factorization problem. The core idea:

  Model: log(ctr) = item_effect + slot_effect

  Every observed CTR for an (item, position, format) cell is modeled as the sum of two independent effects:
  - item_effect -- how inherently clickable the item is (quality, topic, headline)
  - slot_effect -- how much the position/format boosts or suppresses clicks (position bias)

  Working in log space makes this additive; in real space it's multiplicative: ctr = item_quality × slot_multiplier.

  Why ALS? You can't solve for both effects simultaneously (chicken-and-egg), so you alternate:

  1. Fix slot effects, solve for item effects: For each item, average log_ctr - slot_effect across all positions that item appeared in (weighted
  by impressions). This gives each item's intrinsic quality, after removing the position bias.
  2. Fix item effects, solve for slot effects: For each (position, format) slot, average log_ctr - item_effect across all items that appeared
  there (weighted by impressions). This gives each slot's position bias, after removing item quality.
  3. Repeat. Each iteration refines both estimates. max_change tracks convergence -- when slot effects stop moving, the decomposition has
  stabilized.

  An older SQL approach estimated position bias from "fresh items" assumed to be randomly ranked, but
  this is no longer the case with inferred personalization that pre-biases fresh items.
"""

import logging
from argparse import ArgumentParser

import numpy as np
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

PER_ITEM_CUTOFF = 20 # After 20, we compute format independent of position
MAX_POSITION = 200
MIN_SLOT_ITEMS = 200
MIN_SLOT_CLICKS = 100
MIN_CELL_IMPRESSIONS = 10
MIN_OUTPUT_IMPRESSIONS = 2000
ALS_ITERATIONS = 30

HISTORICAL_IMPRESSIONS_SQL = """
WITH
private_pings AS (
  SELECT
    DATE(submission_timestamp) AS date,
    submission_timestamp,
    document_id,
    events,
    metrics.string.newtab_content_country AS country,
    metrics.object.newtab_content_inferred_interests AS inferred_interests,
    CAST(metrics.quantity.newtab_content_utc_offset AS NUMERIC) AS raw_offset
  FROM `moz-fx-data-shared-prod.firefox_desktop.newtab_content_live`
  WHERE submission_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
    AND metrics.string.newtab_content_country IN ("US")
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
    UPPER(dp.country) AS country,
    TIMESTAMP_TRUNC(dp.submission_timestamp, HOUR) AS event_hour,
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
    fne.event_hour,
    fne.corpus_item_id,
    fne.country,
    fne.position,
    fne.format,
    fne.section_position,
    SUM(CASE WHEN fne.event_name = 'impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN fne.event_name = 'click' THEN 1 ELSE 0 END) AS clicks
  FROM flattened_newtab_events fne
  WHERE SAFE_CAST(fne.position AS INT64) <= 200
  GROUP BY
    fne.event_hour,
    fne.corpus_item_id,
    fne.country,
    fne.position,
    fne.format,
    fne.section_position
),

section_events AS (
  SELECT
    rw.event_hour,
    rw.corpus_item_id,
    rw.country,
    rw.position,
    rw.format,
    rw.section_position,
    rw.impressions,
    rw.clicks
  FROM raw_grouped_totals rw
  WHERE rw.section_position IS NOT NULL
),

adjusted_totals AS (
  SELECT
    event_hour,
    corpus_item_id,
    country,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    position,
    section_position,
    format
  FROM section_events
  GROUP BY
    event_hour,
    corpus_item_id,
    country,
    position,
    format,
    section_position
)
SELECT
  att.event_hour AS time_hour,
  att.corpus_item_id,
  att.country,
  att.impressions,
  att.clicks,
  att.format,
  SAFE_CAST(att.position AS INT64) AS position,
  att.section_position,
  SAFE_DIVIDE(att.clicks, att.impressions) AS ctr
FROM adjusted_totals att
"""


def fetch_historical_impressions(client):
    """Run the historical impressions query and return a DataFrame."""
    log.info("Fetching historical impressions from BigQuery...")
    df = client.query(HISTORICAL_IMPRESSIONS_SQL).to_dataframe()
    log.info(f"Fetched {len(df):,} rows")
    return df


def compute_weights(hist):
    """Compute propensity weights using lattice decomposition with ALS.

    Returns a DataFrame with columns: position, tile_format, impressions, weight.
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
    slot_stats = cell_bridge.reset_index().groupby(["position", "format"]).agg(
        n_items=("corpus_item_id", "nunique"),
        total_clicks=("clicks", "sum"),
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
        cell_fit.apply(lambda r: (r["position"], r["format"]) in reliable_slots, axis=1)
    ].copy()

    # Log-space decomposition requires positive CTR
    cell_fit = cell_fit[cell_fit["ctr"] > 0].copy()
    cell_fit["log_ctr"] = np.log(cell_fit["ctr"])

    log.info(
        f"Lattice: {len(reliable_slots)} reliable slots, "
        f"{len(bridge_items):,} bridge items, {len(cell_fit):,} cells"
    )

    # === Phase 2: Alternating least squares in log space ===
    # Model: log(ctr) = item_effect + slot_effect
    slot_effect = {}
    item_effect = {}

    for iteration in range(ALS_ITERATIONS):
        # Item effects
        cell_fit["slot_eff"] = cell_fit.apply(
            lambda r: slot_effect.get((r["position"], r["format"]), 0), axis=1
        )
        item_eff = cell_fit.groupby("corpus_item_id").apply(
            lambda g: np.average(g["log_ctr"] - g["slot_eff"], weights=g["impressions"]),
            include_groups=False,
        )
        item_effect = item_eff.to_dict()

        # Slot effects
        cell_fit["item_eff"] = cell_fit["corpus_item_id"].map(item_effect)
        new_slot = cell_fit.groupby(["position", "format"]).apply(
            lambda g: np.average(
                g["log_ctr"] - g["item_eff"], weights=g["impressions"]
            ),
            include_groups=False,
        )

        max_change = max(
            abs(new_slot.get(k, 0) - slot_effect.get(k, 0)) for k in reliable_slots
        )
        slot_effect = new_slot.to_dict()

        if iteration < 2 or iteration == ALS_ITERATIONS - 1:
            log.info(f"  ALS iter {iteration}: max_change={max_change:.10f}")

    # Convert log slot effects to multipliers
    slot_multiplier = {k: np.exp(v) for k, v in slot_effect.items()}

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

    # === Phase 4: Normalize weights ===
    all_agg = (
        hist[hist["position"] <= MAX_POSITION]
        .groupby(["position", "format"])[["impressions", "clicks"]]
        .sum()
    )

    norm_data = all_agg.join(merged[["unnormalized_weight"]], how="inner")

    denom_sum = (norm_data["impressions"] / norm_data["unnormalized_weight"]).sum()
    total_impressions = all_agg["impressions"].sum()
    total_clicks = all_agg["clicks"].sum()
    target_ctr = total_clicks / total_impressions

    normalization_factor = target_ctr * denom_sum / norm_data["clicks"].sum()

    merged["weight"] = merged["unnormalized_weight"] * normalization_factor

    log.info(
        f"Normalization: target_ctr={target_ctr:.6f}, factor={normalization_factor:.4f}"
    )

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
            weighted_sum += r["weight"] * real_imp
            imp_sum += real_imp
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

    return output[["section_position", "position", "tile_format", "impressions", "weight"]]


def main():
    """Entry point."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--destination_dataset", default="telemetry_derived")
    parser.add_argument("--destination_table", default="newtab_merino_propensity_v1")
    args = parser.parse_args()

    client = bigquery.Client(args.project)

    hist = fetch_historical_impressions(client)
    result = compute_weights(hist)

    destination = f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    log.info(f"Writing {len(result)} rows to {destination} (WRITE_TRUNCATE)")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(result, destination, job_config=job_config)
    job.result()

    log.info("Done.")


if __name__ == "__main__":
    main()
