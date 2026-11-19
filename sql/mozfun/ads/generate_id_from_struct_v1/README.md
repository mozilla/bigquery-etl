### Usage:

```sql
ads.generate_id_from_struct_v1(
    STRUCT(
        DATE "YYYY-MM-DD", "product", "surface", "country_code", "advertiser", position
        )
    )
)

IDs can be regenerated as long as the fields name, content and position remain stable.

Note: most Ads Data Team modeling that consumes this UDF (e.g. `ad_metrics`, `admarketplace_rates`, `equativ_*`, `kevel_*`, `forecast_*`, `campaign_metrics_daily`) lives in the private `mozilla/private-bigquery-etl` repo, not in the public `bigquery-etl` repo.
