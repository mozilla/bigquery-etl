# cohort_daily_churn_v1

## Description

A daily, Glean‑based cohort churn table that references **all clients first seen in the last 180 days** and includes their attributes for analysis. For each cohort_date (first_seen_date), the model tracks post‑acquisition return behavior and exposes counts needed for churn/retention analyses.

**Churn definition (per table fields):**

* Captures the number of **clients_returned** on **day 1** (the day after first seen), and **between day 1 and day 2, day 1 and day 3, day 1 and day 4, day 1 and day 5, day 1 and day 6, day 1 and day 7, and day 1 and day 28** after the cohort_date. These intervals enable standard short‑term retention/churn calculations off of day‑1 anchoring.

**Filters / scope:**

* Excludes **Firefox Desktop BrowserStack** and **Firefox Desktop MozillaOnline** distributions from all counts.

This model is part of the Glean `glean_telemetry_derived` churn/retention suite