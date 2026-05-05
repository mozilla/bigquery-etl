CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.default_browser_agent.default_browser_agg_by_os`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.default_browser_agent_derived.default_browser_agg_by_os_v1`
