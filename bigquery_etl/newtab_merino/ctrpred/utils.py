"""Small data-shaping helpers for CTR prediction."""

import numpy as np

from bigquery_etl.newtab_merino.ctrpred.infer_ctr import N_BUCKETS


def build_model_input(timeline_df):
    """Turn long-form timeline rows into the model's [a, 144, 2] array."""
    corpus_item_ids = timeline_df["corpus_item_id"].unique()
    counts = np.zeros((len(corpus_item_ids), N_BUCKETS, 2), dtype=float)

    # Keep the item order alongside the array. The caller will use it to map
    # model output back to the JSON rows.
    item_index = {
        corpus_item_id: index for index, corpus_item_id in enumerate(corpus_item_ids)
    }

    for row in timeline_df.itertuples(index=False):
        item = item_index[row.corpus_item_id]
        counts[item, row.bucket, 0] = row.clicks
        counts[item, row.bucket, 1] = max(row.adjusted_impressions, row.clicks)

    return corpus_item_ids, counts


def replace_ctrpred_treatment_rows(json_array, replacement_keys, pseudo_counts):
    """Replace treatment counts in the artifact with predicted pseudo-counts."""
    replacements = {
        key: (clicks, impressions)
        for key, clicks, impressions in zip(
            replacement_keys,
            pseudo_counts.clicks,
            pseudo_counts.impressions,
        )
    }

    for row in json_array:
        key = (row["corpus_item_id"], row["region"])
        if key not in replacements:
            continue

        clicks, impressions = replacements[key]
        if not np.isfinite(clicks) or not np.isfinite(impressions):
            continue

        row["click_count"] = int(round(clicks))
        row["impression_count"] = int(round(impressions))

    return json_array
