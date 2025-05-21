## Is Default UI UDF

This function computes if the newtab opened by the client is attributed to the Default UI.
```
Input parameters from the newtab ping:
       event_category as STRING (category)
       event_name as STRING (name)
       event_details as ARRAY<STRUCT<key STRING, value STRING>> (extra)
       newtab_homepage_category as STRING (metrics.string.newtab_homepage_category)
       newtab_newtab_category as STRING (metrics.string.newtab_newtab_category)
```
