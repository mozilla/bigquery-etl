# cx_normalize_product

Standardizes product names so they are consistent across domains (Product, Glean, etc.), implementing the canonical product mapping defined by the Customer Experience team.

```
mozfun.customer_experience.cx_normalize_product(raw STRING, source STRING) -> STRING
```

`source` selects the rule set: `'GA4'`, `'Zendesk'`, or `'Kitsune'`. An unknown `source` falls through to `'unclassified'`.

> This is a separate UDF from `mozfun.customer_experience.normalize_product`. That one maps unknown inputs to `'Other'` and is relied on by the existing `sumo_metrics_derived` consumers; it is intentionally left unchanged. `cx_normalize_product` adopts the stricter semantics below.

## Return values

- `NULL` when `raw` is `NULL` — there was no value in the source.
- A canonical product name (e.g. `'Firefox Desktop'`, `'Fenix'`, `'Firefox iOS'`, `'Firefox Focus'`, `'Mozilla VPN'`) for recognized inputs.
- `'Other'` for inputs that match a **known-but-unsupported** product (e.g. Pocket, Hubs, contributor pages) — a deliberate bucket, not a failure to recognize.
- `'unclassified'` for any input we **cannot recognize**. This is the bucket to monitor: a value here means a new raw slug appeared that needs a mapping rule.

The full canonical-name set is defined by the `THEN` clauses in [`udf.sql`](./udf.sql).

## Rules by source

### Zendesk and Kitsune

Exact-match only. Each `raw` slug maps to one canonical product; deliberately-grouped slugs return `'Other'` and unknown slugs return `'unclassified'`. New slugs are added by extending the `WHEN source = '<…>' AND raw IN (…)` arms in [`udf.sql`](./udf.sql).

### GA4

Two layers, evaluated in order. **Ordering is load-bearing** — do not reorder without re-reading the test cases at the bottom of [`udf.sql`](./udf.sql).

1. **Exact-match path list.** Preserves the historical path → product bucketing from the original CX mapping.
2. **Substring fallback.** Applied only when no exact-match fires, so newly-coined paths map automatically without a code change.

Substring precedence (top wins):

1. `/contributor/` → `'Other'` — bucketed first so e.g. `/contributor/thunderbird/` does not leak into Thunderbird.
2. Deprecated/non-product brand markers (`firefox-os`, `firefox-lite`, `firefox-reality`, `firefox-lockwise`, `firefox-fire-tv`, `firefox-amazon-devices`, `firefox-send`, `firefox-windows-8-touch`, `webmaker`, `pocket`, `hubs`, `screenshot-go`, `open-badges`) → `'Other'`. The empty/malformed path `//` is also bucketed `'Other'`.
3. `/mozilla-account/` → `'Mozilla Account'`.
4. `/mozilla-vpn/`, `/firefox-private-network/`, `/firefox-private-network-vpn/` → `'Mozilla VPN'`.
5. `/relay/` → `'Firefox Relay'`.
6. `/monitor/` → `'Mozilla Monitor'`.
7. `/mdn-plus/` → `'MDN Plus'`.
8. `/focus-firefox/`, `/klar/` → `'Firefox Focus'`.
9. `/thunderbird-android/` → `'Thunderbird Android'` (more specific, fires before generic Thunderbird).
10. `/thunderbird/` → `'Thunderbird'`.
11. `/firefox-enterprise/` → `'Firefox Enterprise'`.
12. `/firefox-android-esr/`, `/firefox-android/` → `'Fenix'`. `mobile` is **not** an Android signal for GA4 (a `/mobile/` path may be iOS); the `mobile` → Fenix assumption applies only to Kitsune. Checked before iOS, so an explicit Android marker wins when a path carries both.
13. `/firefox-ios/`, `/ios/` → `'Firefox iOS'`.
14. `/firefox-preview/` → `'Firefox Desktop'` Firefox Preview maps to Firefox Desktop.
15. `/firefox/` → `'Firefox Desktop'`.
16. Catch-all → `'unclassified'`.

The most-specific brand markers always fire before broader ones (e.g. `mozilla-vpn` before any generic Firefox match) so paths like `/firefox/mozilla-vpn/` resolve to `'Mozilla VPN'`, not `'Firefox Desktop'`.

## Adding a new slug

When a raw value surfaces as `'unclassified'`:

1. Decide the canonical product the slug belongs to.
2. For Zendesk/Kitsune, add the slug to the relevant exact-match arm in [`udf.sql`](./udf.sql). For GA4, prefer extending the substring fallback if the slug shares a brand marker with existing paths; otherwise add it to the exact-match list.
3. Add a test case at the bottom of [`udf.sql`](./udf.sql).
4. If the slug is a non-product page that should be grouped rather than recognized, route it to `'Other'` instead.
