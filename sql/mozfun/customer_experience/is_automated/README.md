# is_automated

Returns TRUE when the aggregated tag set contains any of the automation / autosolve
/ experiment-macro tags maintained by the Zendesk / Customer Experience teams.

- **TRUE** — at least one automation tag is present.
- **FALSE** — tags exist but none match (including the empty array).
- **NULL** — the input array itself is `NULL` (input data missing) — distinct from an empty tag set.

## Tag list

The tags captured cover Self-Service Automation (SSA) flows, Appbot autosolve, loginless autosolve, and the SSA experiment macro / star variants. Tickets matching any of these were resolved or rated through automation rather than agent handling.

The authoritative list lives in the UDF body — see [`udf.sql`](./udf.sql).

## Usage

Per-ticket flag — caller aggregates `tag` rows per `ticket_id`:

```sql
SELECT
  ticket_id,
  mozfun.customer_experience.is_automated(ARRAY_AGG(tag)) AS is_automated
FROM `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
GROUP BY ticket_id
```

If a 1/0 column is needed downstream (matching the original `automation_class` CTE):

```sql
SELECT
  ticket_id,
  IF(mozfun.customer_experience.is_automated(ARRAY_AGG(tag)), 1, 0) AS automation_class
FROM `moz-fx-data-shared-prod.zendesk_syndicate.ticket_tag`
GROUP BY ticket_id
```
