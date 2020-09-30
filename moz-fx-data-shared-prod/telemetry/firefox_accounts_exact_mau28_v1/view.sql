CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_accounts_exact_mau28_v1`
AS
SELECT
  submission_date,
  SUM(mau) AS mau,
  SUM(wau) AS wau,
  SUM(dau) AS dau,
  SUM(seen_in_tier1_country_mau) AS tier1_mau
FROM
  `moz-fx-data-shared-prod.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
GROUP BY
  submission_date
