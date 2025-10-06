# cohort_weekly_statistics_v1

## Description

A weekly Glean‑based cohort statistics table. For each **`cohort_week`** (the week of first seen, defined as the **preceding Sunday** of a client’s first_seen_date) and **`activity_week`** (the **first_date_of_the_week**, also a **Sunday**), this model summarizes cohort size and activity at a weekly cadence.

Includes one row per `(cohort_week, activity_week, and other attributes)` within the analysis window and includes:

* **Count of clients in the cohort** (cohort size for that cohort_week).
* **Count of clients active in that week** (within that cohort).
* **`weeks_after_first_seen_week`** — integer offset of `activity_week` from `cohort_week` (0 = cohort week).

**Filters / scope:**

* Inherits exclusions from rolling cohorts: **Firefox Desktop BrowserStack** and **Firefox Desktop MozillaOnline** are **excluded** from all counts.

> Week boundaries are **Sunday‑based** for both `cohort_week` and `activity_week`.