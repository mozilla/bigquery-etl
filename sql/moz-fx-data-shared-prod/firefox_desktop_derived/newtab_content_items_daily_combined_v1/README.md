# newtab_content_items_daily_combined

## Description
A view of the combined (Newtab + Newtab-Content) daily aggregation of newtab content actions on content/items, joined with the latest corpus item details from the corpus_items_current table so that the most current values for the corpus item are available.

Previously, all content data was coming in via the newtab ping. With the addition of the newtab-content ping, we needed to be able to aggregate and combine these data sources.

As of 2025-09-16, the newtab-content ping was rolled out to all users. So most data should now be coming through this ping exclusively, but we will still need to keep the newtab modeling as part of this for historical analysis.