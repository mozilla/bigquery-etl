## Surface ID Country UDF

This function derives `country` from `surface_id` string, particularly when `country` is `null`.

In general, it should be used for content recommendations in the `newtab_content` ping, which is based on country rather than surface.

For tables:

- `moz-fx-data-shared-prod.firefox_desktop_....newtab_v1`
- `moz-fx-data-shared-prod.firefox_desktop_....newtab_content_v1`
