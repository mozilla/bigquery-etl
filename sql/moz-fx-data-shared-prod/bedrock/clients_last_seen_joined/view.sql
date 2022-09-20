CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.bedrock.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.bedrock_derived.clients_last_seen_joined_v1`
