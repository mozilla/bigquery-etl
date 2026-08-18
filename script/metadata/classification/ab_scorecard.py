"""Quality scorecard for the model A/B: score each model's FxA classifications
against the vetted expectations (see MODEL_AB_TESTING.md).

Reads the dumped rows (default /tmp/fxa_ab_all.json: model, source_table,
column_name, data_type, primary_label, secondary_labels, confidence,
needs_review) and reports, per model: coverage, sensitive-column
under-classification (hard gate), *.fxid over-assignment, needs_review
calibration, intra-model consistency, and agreement with the cross-model
consensus.

Usage: python ab_scorecard.py [dump.json]
"""

import json
import sys
from collections import Counter, defaultdict

PATH = sys.argv[1] if len(sys.argv) > 1 else "/tmp/fxa_ab_all.json"

# Direct-PII columns that must land in a sensitive tier (leaf name -> acceptable
# label substrings). From the vetted FxA analysis.
SENSITIVE_EXPECT = {
    "email": ("contact", "email"),
    "normalizedemail": ("contact", "email"),
    "ipaddr": ("ip_address", "ip"),
    "ipaddrhmac": ("ip_address", "ip"),
    "phonenumber": ("contact", "phone"),
    "lookupdata": ("contact", "phone", "location"),
    "taxaddress": ("location", "address", "financial"),
}
# Any of these substrings => a "sensitive tier" label.
SENSITIVE_TIERS = ("contact", "ip_address", "location", "financial", "demographic")
# *.fxid is only justified on the actual account identifier.
FXID_OK_NAMES = {"uid", "userid"}


def leaf(name):
    return name.split(".")[-1].lower()


def is_true(v):
    return str(v).lower() == "true"


def main():
    """Compute and print the model A/B quality scorecard."""
    rows = json.load(open(PATH))
    models = sorted({r["model"] for r in rows})
    # by_model[model][(table, column)] = row
    by_model = defaultdict(dict)
    for r in rows:
        by_model[r["model"]][(r["source_table"], r["column_name"])] = r
    all_keys = sorted({(r["source_table"], r["column_name"]) for r in rows})

    # cross-model consensus label per column (majority vote)
    consensus = {}
    for k in all_keys:
        labels = [by_model[m][k]["primary_label"] for m in models if k in by_model[m]]
        consensus[k] = Counter(labels).most_common(1)[0][0]

    sens = {m: [] for m in models}  # violations: (col, label)
    fxid = {m: [] for m in models}  # over-assignment: (col, type, label)
    review = {m: [] for m in models}  # needs_review columns
    inconsist = {m: [] for m in models}  # column names with >1 label
    agree = {m: 0 for m in models}  # matches consensus
    covered = {m: 0 for m in models}

    for m in models:
        rowmap = by_model[m]
        covered[m] = len(rowmap)
        # per-leaf label sets for intra-model consistency
        leaf_labels = defaultdict(set)
        for (table, col), r in rowmap.items():
            lbl = r["primary_label"] or ""
            lf = leaf(col)
            leaf_labels[lf].add(lbl)
            # sensitive gate
            if lf in SENSITIVE_EXPECT and not any(
                s in lbl for s in SENSITIVE_EXPECT[lf]
            ):
                sens[m].append((f"{table}.{col}", lbl))
            # fxid over-assignment
            if lbl.endswith("fxid") and lf not in FXID_OK_NAMES:
                fxid[m].append((f"{table}.{col}", r.get("data_type"), lbl))
            # needs_review
            if is_true(r.get("needs_review")):
                review[m].append(f"{table}.{col}")
            # consensus agreement
            if lbl == consensus[(table, col)]:
                agree[m] += 1
        for lf, labels in leaf_labels.items():
            if len(labels) > 1:
                inconsist[m].append((lf, sorted(labels)))

    # ---- scorecard table ----
    print(f"\n=== MODEL A/B QUALITY SCORECARD  (n={len(all_keys)} columns) ===\n")
    hdr = (
        f"{'model':<26} {'cov':>4} {'sens_viol':>9} {'fxid_over':>9} "
        f"{'needs_rev':>9} {'inconsist':>9} {'vs_consensus':>13}"
    )
    print(hdr)
    print("-" * len(hdr))
    for m in models:
        cov = covered[m]
        print(
            f"{m:<26} {cov:>4} {len(sens[m]):>9} {len(fxid[m]):>9} "
            f"{len(review[m]):>9} {len(inconsist[m]):>9} "
            f"{f'{agree[m]}/{cov} ({100*agree[m]//cov}%)':>13}"
        )

    print(
        "\nsens_viol   = direct-PII columns NOT in a sensitive tier (hard gate; lower=better)"
    )
    print(
        "fxid_over   = timestamps/flags/PKs mislabeled user.account.fxid (lower=better)"
    )
    print("needs_rev   = columns the model flagged for review")
    print(
        "inconsist   = column names given >1 different label across tables (lower=better)"
    )
    print(
        "vs_consensus= agreement with the majority label across models (higher=better)"
    )

    # ---- detail sections ----
    def detail(title, data, fmt):
        print(f"\n--- {title} ---")
        for m in models:
            items = data[m]
            if items:
                print(f"  {m}: {len(items)}")
                for it in items[:12]:
                    print(f"      {fmt(it)}")
                if len(items) > 12:
                    print(f"      ...(+{len(items)-12})")
            else:
                print(f"  {m}: none")

    detail(
        "SENSITIVE under-classification (hard gate)",
        sens,
        lambda it: f"{it[0]}  ->  {it[1]}",
    )
    detail("*.fxid over-assignment", fxid, lambda it: f"{it[0]} [{it[1]}]  ->  {it[2]}")
    detail("needs_review flags", review, lambda it: it)
    detail(
        "intra-model inconsistency (column -> labels)",
        inconsist,
        lambda it: f"{it[0]}: {it[1]}",
    )


if __name__ == "__main__":
    main()
