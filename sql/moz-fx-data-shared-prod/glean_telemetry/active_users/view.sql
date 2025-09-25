CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_telemetry.active_users`(
    submission_date
    OPTIONS
      (description = "The date when the telemetry ping is received on the server side."),
      client_id
    OPTIONS
      (description = "A unique identifier (UUID) for the client."),
      sample_id
    OPTIONS
      (
        description = "A number, 0-99, that samples by client_id and allows filtering data for analysis. It is a pipeline-generated artifact that should match between pings."
      ),
      app_name
    OPTIONS
      (
        description = "Application Name - For example, Firefox iOS, Fenix, Firefox Desktop, Klar iOS, etc."
      ),
      days_seen_bits
    OPTIONS
      (
        description = "Bit pattern representing whether the client sent a baseline ping for each of the last 28 days, inclusive of the submission date"
      ),
      days_active_bits
    OPTIONS
      (
        description = "Bit pattern representing whether the client was active for each of the last 28 days, inclusive of the submission date"
      ),
      is_dau
    OPTIONS
      (
        description = "Is Daily Active User - Indicates if the client is considered an active user on this date."
      ),
      is_wau
    OPTIONS
      (
        description = "Is Weekly Active User - Indicates if the client is considered an active user in the last 7 days, inclusive of the submission date."
      ),
      is_mau
    OPTIONS
      (
        description = "Is Monthly Active User - Indicates if the client is considered an active user in the last 28 days, inclusive of the submission date."
      ),
      is_daily_user
    OPTIONS
      (
        description = "Is Daily User - Indicates if the client reported a baseline ping on this date"
      ),
      is_weekly_user
    OPTIONS
      (
        description = "Is Weekly User - Indicates if the client reported a baseline ping in the last 7 days, inclusive of the submission date."
      ),
      is_monthly_user
    OPTIONS
      (
        description = "Is Monthly User - Indicates if the client reported a baseline ping in the last 28 days, inclusive of the submission date."
      ),
      is_desktop
    OPTIONS
      (description = "Indicates if the client is included in the desktop KPI"),
      is_mobile
    OPTIONS
      (description = "Indicates if the client is included in the mobile KPI")
  )
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  app_name,
  days_seen_bits,
  days_active_bits,
  is_dau,
  is_wau,
  is_mau,
  is_daily_user,
  is_weekly_user,
  is_monthly_user,
  is_desktop,
  FALSE AS is_mobile
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
WHERE
  client_id <> '00000000-0000-0000-0000-000000000000' --exclude nil client ID
UNION ALL
SELECT
  submission_date,
  client_id,
  sample_id,
  app_name,
  days_seen_bits,
  days_active_bits,
  is_dau,
  is_wau,
  is_mau,
  is_daily_user,
  is_weekly_user,
  is_monthly_user,
  FALSE AS is_desktop,
  is_mobile
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_active_users`
WHERE
  client_id <> '00000000-0000-0000-0000-000000000000' --exclude nil client ID
