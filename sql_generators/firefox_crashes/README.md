# Desktop Crashes

Generate a view that unions different sources for Firefox desktop crash pings.
Schemas for the different tables should be equivalent but fields may be in different order or 
with removed metrics missing from newer tables.
If fields do not match, the output will be a union of all the fields.

## crash_live views

Generates a `<app_name>.crash_live` view for some Firefox apps
(`firefox_desktop`, `firefox_crashreporter`, `fenix`, `focus_android`, `klar_android`).
These read the `<bq_dataset_family>_live.crash_v1` tables so consumers can query crash
pings from the current day, at the cost of no deduplication. 

The views exist to grant read access to the live tables through authorized views, since
direct access to live tables is generally not granted.
