-- Query for fxresearch_derived.fxqur_2024okr_desktop_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  month_year,
  okr_groups,
  csat_high,
  csat_low,
  month_tot,
  csat_prop
FROM
  `mozdata.analysis.fxqur_2024okr_desktop`