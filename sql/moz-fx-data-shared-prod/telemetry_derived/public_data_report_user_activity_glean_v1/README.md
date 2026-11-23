# public_data_report_user_activity_glean_v1

The weekly Glean user activity figures behind data.firefox.com's user activity pages, which the site uses from the week of 2027-02-01.

## Grain

One row per week and country name, plus a `Worldwide` row per week. `submission_date` is the week's Monday. Every country with Desktop or mobile MAU that week gets a row. A country code with several names in `static.country_names_v1` gets a row for each name, with the same figures.

## Main fields

- `mau`: monthly active users (`mau_desktop` plus `mau_mobile`)
- `mau_desktop`: Firefox Desktop and Firefox Desktop MozillaOnline clients active in the 28 days ending the week's Monday
- `mau_mobile`: Fenix, Fenix MozillaOnline, Firefox iOS, Focus Android and Focus iOS clients active in the 28 days ending the week's Monday
- `top_locales`: Firefox Desktop's top five full locales, such as `en-US`, among weekly active clients with a locale in the 1% sample, counted on the week's Sunday

## How to read the values

- Figures are exact. The user activity export in `firefox-public-data-report-etl` may apply small-count suppression.
- MAU is read on the week's Monday and top languages on its Sunday, as in `public_data_report_user_activity_v1`.
- A country with no locale clients that week has an empty `top_locales`.
- A client with no country counts under `'??'`, which counts in `Worldwide` and isn't on the site's country list.
- The six other legacy figures, such as `avg_hours_usage_daily` and `top_addons`, have no column here; `telemetry.public_data_report_user_activity` adds them as `NULL`, or empty for `top_addons`, on Glean weeks.
- Compared with `public_data_report_user_activity_v1`:
  - Desktop MAU comes from Glean and includes MozillaOnline, as does Fenix
  - a missing locale is dropped rather than ranked
  - locale shares are out of clients with a locale

## Sources

- `glean_telemetry.active_users_aggregates`, for `mau`, `mau_desktop` and `mau_mobile`
- `firefox_desktop.baseline_clients_last_seen`, 1% sample, for each client's full locale and country
- `firefox_desktop.baseline_active_users`, 1% sample, for which Desktop clients are weekly active
- `static.country_names_v1`, for country names

## Downstream

`telemetry.public_data_report_user_activity` reads it for weeks from its boundary.

## Runs

Weekly, every Monday at 01:00 UTC, in the `firefox_public_data_report` DAG in `telemetry-airflow`. Each run overwrites only the partition for the week starting `ds`.

## History

To be backfilled from 2026-10-05.
