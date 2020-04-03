CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_accounts_exact_mau28`
AS
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_v1`
GROUP BY
  submission_date
