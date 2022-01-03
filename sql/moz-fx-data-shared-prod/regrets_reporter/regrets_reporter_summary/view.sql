CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.regrets_reporter.regrets_reporter_summary`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.regrets_reporter_derived.regrets_reporter_summary_v1`
