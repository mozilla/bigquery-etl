# classify_appbot_group

Classify a Zendesk ticket by its aggregated tags into an Appbot ticket group.

Returns `STRUCT<ticket_group STRING, is_bot BOOL, is_english BOOL>`:

- `ticket_group` — `'Appbot - English'` when `'bot'` is present alongside any English-locale tag; `'Appbot - Non-English'` when `'bot'` is present without any English-locale tag; `'Other'` when no `'bot'` tag is present.
- `is_bot` — TRUE when `'bot'` is in the tag set, otherwise FALSE.
- `is_english` — TRUE when at least one English-locale tag is in the set, otherwise FALSE.

The authoritative English-locale tag list lives in the UDF body — see [`udf.sql`](./udf.sql).

The whole STRUCT is `NULL` when the input array itself is `NULL` (input data missing) — distinct from an empty tag set, where `is_bot` and `is_english` are FALSE and `ticket_group` is `'Other'`.

## Usage

Per-ticket classification — caller aggregates `tag` rows per `ticket_id` and dots into the struct:

```sql
SELECT
  ticket_id,
  mozfun.customer_experience.classify_appbot_group(ARRAY_AGG(tag)).ticket_group
FROM `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
GROUP BY ticket_id
```

Note: `ARRAY_AGG(tag)` may include `NULL` elements; the UDF treats them as non-matching. A `NULL` input array (missing data) returns a `NULL` struct, while an empty array (no tags) returns `is_bot = FALSE`, `is_english = FALSE`, `ticket_group = 'Other'`.

If you want all details and want to avoid invoking the UDF three times per row, alias the struct once in a CTE and dot into it downstream:

```sql
WITH classified AS (
  SELECT
    ticket_id,
    mozfun.customer_experience.classify_appbot_group(ARRAY_AGG(tag)) AS appbot
  FROM `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
  GROUP BY ticket_id
)
SELECT
  ticket_id,
  appbot.ticket_group,
  appbot.is_bot,
  appbot.is_english
FROM classified
```
