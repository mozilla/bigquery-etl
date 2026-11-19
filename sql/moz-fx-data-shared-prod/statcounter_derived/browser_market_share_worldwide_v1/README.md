# browser_market_share_worldwide_v1

Daily, worldwide browser market share percentages sourced from Statcounter. Each row represents the market share of a single browser on a given date, expressed as a percentage of total web traffic observed by Statcounter.

## Schema

| Column | Type | Description |
| --- | --- | --- |
| `sk` | STRING | Surrogate key; MD5 hash of date, geography, device, and browser |
| `date` | DATE | Date of observation |
| `geography` | STRING | Geographic region |
| `device` | STRING | Device type (Desktop or Mobile) |
| `browser` | STRING | Browser name |
| `percent` | FLOAT | Market share percentage |

## Primary Key

`date`, `geography`, `device`, `browser` (geography is always "Worldwide")

## Source

`../pipeline.py` fetches data daily from two Statcounter public CSV download URLs (one per device type, no auth required) and loads it into BigQuery.

## Grain

The schema is fixed. Statcounter's public CSV export only provides data at the date, geography, device, and browser grain — no finer breakdown is available from the source. Geography is always "Worldwide" in this table.
