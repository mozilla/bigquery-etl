# cohort_daily_statistics_v1

## Description

A daily Glean‑based cohort statistics table. For each `cohort_date` (the day a client is first seen) and `activity_date` (the submission date), this model summarizes cohort size and activity over the first 180 days of a cohort.

* **`cohort_date`** = client first_seen_date.
* **`activity_date`** = submission_date (the day being measured).
* Includes rows only where `activity_date` is within **180 days** (inclusive) of `cohort_date`.

Provided counts per `(cohort_date, activity_date, and other attributes)` include:

* **Count of clients in the cohort** (cohort size).
* **Count of clients active on that day** (within that cohort).

**Filters / scope:**

* This table is sourced from rolling cohorts logic that **excludes Firefox Desktop BrowserStack** and **Firefox Desktop MozillaOnline**; the same exclusions apply here.
