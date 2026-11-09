CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.deepnote.audit_log`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
