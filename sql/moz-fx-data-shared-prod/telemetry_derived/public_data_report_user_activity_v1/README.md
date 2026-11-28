# public_data_report_user_activity_v1

The weekly user activity figures behind data.firefox.com's User Activity and Usage Behavior pages.

## Grain

One row per week and country name, plus a `Worldwide` row per week. `submission_date` is the week's Monday. A country code with several names in `static.country_names_v1` gets a row for each name, with the same figures.

## Main fields

- `mau`: monthly active users, counting clients active in the 28 days ending the week's Monday, for Firefox Desktop, Fenix, Firefox iOS, Focus Android and Focus iOS
- `mau_desktop`: the Firefox Desktop part of `mau`; 0 if a country has no Desktop clients
- `mau_mobile`: the mobile part of `mau`, equal to `mau` minus `mau_desktop`, covering Fenix, Firefox iOS, Focus Android and Focus iOS
- `dau`: daily active users, averaged over the 28 days ending the week's Monday, the same days as `mau` and the same five apps. Each app is averaged over its days with data in any country, so a day without clients in one country counts as 0 there, and a day with no data for an app is left out. The apps' averages are added and rounded to a whole number.
- `avg_hours_usage_daily`, `intensity`, `new_profile_rate` and `latest_version_ratio`: Desktop usage figures from the 1% sample
- `top_addons` and `has_addon_ratio`: Desktop add-on figures from the 1% sample
- `top_locales`: Desktop's top five locales by share of clients seen in the week in the 1% sample, counted on the week's Sunday

## How to read the values

- `mau_desktop` plus `mau_mobile` always equals `mau`.

## Sources

- `telemetry.active_users_aggregates`, for `mau`, `mau_desktop`, `mau_mobile` and `dau`
- `telemetry.clients_last_seen`, 1% sample, for the usage, add-on and locale figures
- `firefox_desktop_stable.metrics_v1`, for add-ons on Firefox 156 and later
- `telemetry.buildhub2`, for the latest release in `latest_version_ratio`
- `static.country_names_v1`, for country names

## Downstream

The `user_activity_export` task in the `firefox_public_data_report` DAG runs the `user_activity` command in `firefox-public-data-report-etl`, which writes `fxhealth.json` and `webusage.json` for the site.

## Runs

Weekly, every Monday at 01:00 UTC, in the `firefox_public_data_report` DAG in `telemetry-airflow`. Each run overwrites only the partition for the week starting `ds`.
