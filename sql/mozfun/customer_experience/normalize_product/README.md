# normalize_product

Maps a raw product identifier to the canonical CX product name. Replaces the static lookup table `moz-fx-data-shared-prod.static.cx_product_mappings_v1`.

```
mozfun.customer_experience.normalize_product(raw STRING, source STRING) -> STRING
```

`source` selects the rule set: `'GA4'`, `'Zendesk'`, or `'Kitsune'`. Unknown `source` values fall through to `'Other'`.

## Return values

- `NULL` when `raw` is `NULL`.
- A canonical product name (e.g. `'Firefox'`, `'Mozilla VPN'`, `'Firefox Android'`) for recognized inputs.
- `'Other'` for any input that does not match a rule — downstream consumers do not need a COALESCE fallback. Treat `'Other'` as the signal to investigate via the drift check (below).

The full canonical-name set is defined by the `THEN` clauses in [`udf.sql`](./udf.sql).

## Rules by source

### Zendesk and Kitsune

Exact-match only. Each `raw` slug maps to one canonical product; unknown slugs return `'Other'`. New slugs are added by extending the `WHEN source = '<…>' AND raw IN (…)` arms in [`udf.sql`](./udf.sql).

### GA4

Two layers, evaluated in order. **Ordering is load-bearing** — do not reorder without re-reading the test cases at the bottom of [`udf.sql`](./udf.sql).

1. **Exact-match path list.** Preserves every mapping that existed in the static lookup table, so existing GA4 paths keep their historical bucketing.
2. **Substring fallback.** Applied only when no exact-match fires, so newly-coined paths (e.g. `/firefox/firefox-enterprise/mobile/`) map automatically without a code change.

Substring precedence (top wins):

1. `/contributor/` → `'Other'` — bucketed first so e.g. `/contributor/thunderbird/` does not leak into Thunderbird.
2. Deprecated/non-product brand markers (`firefox-os`, `firefox-lite`, `firefox-reality`, `firefox-lockwise`, `firefox-fire-tv`, `firefox-amazon-devices`, `firefox-send`, `firefox-windows-8-touch`, `firefox-preview`, `webmaker`, `pocket`, `hubs`, `screenshot-go`, `open-badges`) → `'Other'`.
3. `/mozilla-account/` → `'Mozilla Account'`.
4. `/mozilla-vpn/`, `/firefox-private-network/`, `/firefox-private-network-vpn/` → `'Mozilla VPN'`.
5. `/relay/` → `'Firefox Relay'`.
6. `/monitor/` → `'Mozilla Monitor'`.
7. `/mdn-plus/` → `'Mdn Plus'`.
8. `/focus-firefox/`, `/klar/` → `'Firefox Focus'`.
9. `/thunderbird-android/` → `'Thunderbird Android'` (more specific, fires before generic Thunderbird).
10. `/thunderbird/` → `'Thunderbird'`.
11. `/firefox-enterprise/` → `'Firefox Enterprise'`.
12. `/firefox-android-esr/`, `/mobile/`, `/ios/` (but not `/firefox-ios/`) → `'Firefox Android'`.
13. `/firefox/` → `'Firefox'`.
14. Catch-all → `'Other'`.

The most-specific brand markers always fire before broader ones (e.g. `mozilla-vpn` before any generic Firefox match) so paths like `/firefox/mozilla-vpn/` resolve to `'Mozilla VPN'`, not `'Firefox'`.

## Adding a new slug

When the drift check (see below) surfaces a raw value mapping to `'Other'`:

1. Decide the canonical product the slug belongs to.
2. For Zendesk/Kitsune, add the slug to the relevant exact-match arm in [`udf.sql`](./udf.sql). For GA4, prefer extending the substring fallback if the slug shares a brand marker with existing paths; otherwise add it to the exact-match list.
3. Add a test case at the bottom of [`udf.sql`](./udf.sql).
4. If the slug is genuinely a non-product page that should stay `'Other'`, add it to the corresponding `NOT IN` exclusion list in the drift check ([`sql/moz-fx-data-shared-prod/sumo_metrics_derived/ga4_engagement_sessions_daily_v1/checks.sql`](../../../moz-fx-data-shared-prod/sumo_metrics_derived/ga4_engagement_sessions_daily_v1/checks.sql)) so it stops firing.

## Drift check

`ga4_engagement_sessions_daily_v1/checks.sql` runs daily and emits a `#warn` when any raw `custom_product` / Kitsune `product` / GA4 `products` event_param maps to `'Other'` and is not already in the intentional `'Other'` exclusion list. New slugs surface as they first appear without needing a history re-scan.
