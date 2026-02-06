Usage:

```sql
SELECT
  mozfun.norm.map_utm_to_channel_group(utm_source) AS channel_group
FROM
  your_table
```

Returns one of the following channel group categories:
- `Marketing Owned` - Mozilla-owned marketing properties
- `Product Owned` - In-product features and Firefox properties
- `Marketing Paid` - Paid advertising channels
- `Direct` - Direct traffic
- `Miscellaneous` - Other or unmapped sources
