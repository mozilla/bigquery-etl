# browser_market_share_regions_v1

Daily browser market share percentages by region sourced from Statcounter. Each row represents the market share of a single browser in a given region on a given date, expressed as a percentage of total web traffic observed by Statcounter.

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

`date`, `geography`, `device`, `browser`

## Source

`../pipeline.py` fetches data daily from Statcounter public CSV download URLs (no auth required, one per region/device combination) and loads it into BigQuery. Regions: Africa, Asia, Europe, North America, Oceania, South America.

## Grain

The schema is fixed. Statcounter's public CSV export only provides data at the device, region, and date grain — no finer breakdown is available from the source.
