-- Daily per-domain rollup of FxA primary email addresses. One row per
-- (as_of_date, registrable_domain).
--
-- Publishes the domain-level shape that the fraud detection scripts already
-- compute privately, so it can be graphed on a broadly readable dashboard
-- without exposing addresses. Accounts are grouped by registrable domain
-- (eTLD+1, via the PSL-aware NET.REG_DOMAIN) rather than by the full host, so
-- subdomain-spray campaigns -- where every account uses a unique throwaway
-- subdomain -- collapse into one row instead of vanishing as thousands of
-- single-account hosts. `distinct_hosts` preserves the fan-out they produce.
--
-- Only domains with activity in the trailing 28 days are emitted. Of the ~121k
-- domains holding 10+ accounts, just ~2.7% gain an account on any given day, so
-- a full daily snapshot would spend 97% of every partition restating unchanged
-- cumulative totals. Restricting to the active set is ~23k rows/day instead of
-- ~121k, and a domain dormant for a month is not a live fraud signal. Note the
-- consequence: a per-domain time series has gaps on quiet stretches, so use the
-- window columns rather than differencing `accounts` across dates. Authoritative
-- account totals live in `monitoring_db_counts_v1`, not here.
--
-- Verified flags reflect state at the 02:30 snapshot, so a registration late on
-- as_of_date has had less time to confirm than one early in the day. Expect a
-- small systematic dip in verified share for late-day cohorts.
--
-- Verified counts are emitted per window as well as cumulatively, because the
-- cumulative ratio is useless as a signal: it sits at ~99.9% for every large
-- provider (gmail.com included), since unverified accounts get cleaned up. It is
-- the verified share of *recent* registrations that separates an attacker
-- confirming their own signups from ordinary traffic.
--
-- Domains with fewer than 10 accounts are pooled into a single
-- `__below_threshold__` row rather than published individually: a personal or
-- vanity domain with one account effectively names its owner. The pooled row is
-- itself a signal -- a spray distributed across thousands of tiny throwaway
-- domains shows up there and nowhere else.
WITH primary_emails AS (
  SELECT
    LOWER(REGEXP_EXTRACT(email, r'@(.+)$')) AS host,
    isVerified,
    createdAt
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1`
    -- Snapshot at 02:30 UTC the morning after as_of_date, not at midnight.
    -- bqetl_accounts_db refreshes this mirror on `30 1/3 * * *`, so a midnight
    -- snapshot only sees what the 22:30 run captured and silently drops the
    -- day's last ~1.5 hours of registrations (~4.7% of a day, measured). Those
    -- rows never reappear either: the next partition counts createdAt >=
    -- as_of_date + 1, so they fall out of new_accounts_1d entirely while still
    -- landing in the 7d/28d windows -- an inconsistency within a single row,
    -- and a blind spot a campaign could sit in. 02:30 is after the 01:30 run
    -- has imported all of as_of_date, and is this ETL's own schedule
    -- (bqetl_accounts_derived, `30 2 * * *`), so it is never in the future.
    FOR SYSTEM_TIME AS OF TIMESTAMP(DATETIME(@as_of_date + 1, TIME '02:30:00'), 'UTC')
  WHERE
    isPrimary = TRUE
    AND email LIKE '%@%'
    -- Reading past midnight to get a complete day means also excluding the next
    -- day's rows, so every count below is bounded to as_of_date.
    AND createdAt < TIMESTAMP(@as_of_date + 1, 'UTC')
),
windowed AS (
  SELECT
    NET.REG_DOMAIN(host) AS registrable_domain,
    host,
    isVerified,
    createdAt,
    createdAt >= TIMESTAMP(@as_of_date, 'UTC') AS in_1d,
    createdAt >= TIMESTAMP(DATE_SUB(@as_of_date, INTERVAL 6 DAY), 'UTC') AS in_7d,
    createdAt >= TIMESTAMP(DATE_SUB(@as_of_date, INTERVAL 27 DAY), 'UTC') AS in_28d
  FROM
    primary_emails
),
per_domain AS (
  SELECT
    registrable_domain,
    COUNT(*) AS accounts,
    COUNTIF(isVerified) AS verified_accounts,
    COUNTIF(in_1d) AS new_accounts_1d,
    COUNTIF(in_1d AND isVerified) AS verified_new_accounts_1d,
    COUNTIF(in_7d) AS new_accounts_7d,
    COUNTIF(in_7d AND isVerified) AS verified_new_accounts_7d,
    COUNTIF(in_28d) AS new_accounts_28d,
    COUNTIF(in_28d AND isVerified) AS verified_new_accounts_28d,
    COUNT(DISTINCT host) AS distinct_hosts,
    COUNT(DISTINCT IF(in_28d, host, NULL)) AS distinct_hosts_28d,
    MIN(createdAt) AS first_account_at,
    MAX(createdAt) AS last_account_at
  FROM
    windowed
  WHERE
    registrable_domain IS NOT NULL
  GROUP BY
    registrable_domain
  HAVING
    new_accounts_28d > 0
)
SELECT
  @as_of_date AS as_of_date,
  registrable_domain,
  accounts,
  verified_accounts,
  new_accounts_1d,
  verified_new_accounts_1d,
  new_accounts_7d,
  verified_new_accounts_7d,
  new_accounts_28d,
  verified_new_accounts_28d,
  distinct_hosts,
  distinct_hosts_28d,
  first_account_at,
  last_account_at,
  CAST(NULL AS INT64) AS domains_pooled
FROM
  per_domain
WHERE
  accounts >= 10
UNION ALL
-- The pooled low-count tail, emitted as one row so a spray across thousands of
-- individually-tiny throwaway domains stays visible in aggregate.
SELECT
  @as_of_date AS as_of_date,
  '__below_threshold__' AS registrable_domain,
  SUM(accounts) AS accounts,
  SUM(verified_accounts) AS verified_accounts,
  SUM(new_accounts_1d) AS new_accounts_1d,
  SUM(verified_new_accounts_1d) AS verified_new_accounts_1d,
  SUM(new_accounts_7d) AS new_accounts_7d,
  SUM(verified_new_accounts_7d) AS verified_new_accounts_7d,
  SUM(new_accounts_28d) AS new_accounts_28d,
  SUM(verified_new_accounts_28d) AS verified_new_accounts_28d,
  SUM(distinct_hosts) AS distinct_hosts,
  SUM(distinct_hosts_28d) AS distinct_hosts_28d,
  MIN(first_account_at) AS first_account_at,
  MAX(last_account_at) AS last_account_at,
  COUNT(*) AS domains_pooled
FROM
  per_domain
WHERE
  accounts < 10
