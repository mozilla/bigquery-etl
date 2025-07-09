### Usage:

```sql
ads.generate_id_from_struct_v1(
    STRUCT(
        DATE "YYYY-MM-DD", "product", "surface", "country_code", "advertiser", position
        )
    )
)

IDs can be regenerated as long as the fields name, content and position remain stable.
