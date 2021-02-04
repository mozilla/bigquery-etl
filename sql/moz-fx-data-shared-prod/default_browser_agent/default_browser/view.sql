CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.default_browser_agent.default_browser`
AS
SELECT
  * EXCEPT (
    client_id  -- This field is deprecated and was never actually sent.
  ) REPLACE(`moz-fx-data-shared-prod.udf.normalize_metadata`(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.default_browser_agent_stable.default_browser_v1`
