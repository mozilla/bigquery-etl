# accounts_backend_derived.delete_events_accounts_db_v1

## Description

One row per account deleted on the submission date, read from the FxA accounts database mirror. Shredder uses it, through the `firefox_accounts.fxa_delete_events` view, to delete data linked to the deleted account.

* **Grain:** `user_id_unhashed`, the account uid. One row per account.
* **Source:** `accounts_db_external.fxa_deleted_accounts_v1`, the daily mirror of the FxA `deletedAccounts` table.
* **Columns:** the same four columns as `firefox_accounts_derived.fxa_delete_events_v2`: `submission_timestamp`, `user_id` (SHA-256 of the uid), `hmac_user_id` (HMAC-SHA-256 of the uid, using the same key as `fxa_delete_events_v2`) and `user_id_unhashed`.
* **Downstream:** the `firefox_accounts.fxa_delete_events` view reads this table for deletions from 2026-11-17 on, and `fxa_delete_events_v1` and `fxa_delete_events_v2` for earlier dates. Those two tables are no longer scheduled.

## Dates

`submission_timestamp` is the account's `deletedAt` time, and each partition is its UTC date. The mirror holds only the current state of the FxA table, so this table keeps a deletion even if FxA later removes it from the source. That holds only until the partition is re-run or backfilled after the source row is gone.

## Runs

One scheduled run per day in `bqetl_accounts_derived`, after the mirror refresh for the same date. Re-running a date rewrites that partition from the current mirror.
