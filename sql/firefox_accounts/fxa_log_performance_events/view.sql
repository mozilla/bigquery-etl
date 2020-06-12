CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_log_performance_events`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_log_content_events_v1`
WHERE
  STARTS_WITH(event, 'flow.performance')
