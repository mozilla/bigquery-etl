CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.enterprise_and_consumer_metric_segment_clients`
AS
SELECT
  *,
  CASE
    WHEN normalized_channel = "release"
      AND ((policies_count > 1) OR (policies_is_enterprise = TRUE))
      THEN "enterprise_release"
    WHEN normalized_channel = "release"
      AND (
        (distribution_id IS NOT NULL)
        OR (policies_count = 0)
        OR (policies_is_enterprise = FALSE)
      )
      THEN "consumer_release"
    WHEN normalized_channel = "esr"
      AND ((policies_count > 1) AND (distribution_id IS NULL))
      THEN "enterprise_esr"
    WHEN normalized_channel = "esr"
      AND (
        (distribution_id IS NOT NULL)
        OR (policies_count = 0)
        OR (policies_is_enterprise = FALSE)
      )
      THEN "consumer_esr"
    WHEN normalized_channel = "release"
      THEN "unknown_release"
    WHEN normalized_channel = "esr"
      THEN "unknown_esr"
    ELSE "unexpected_classification" -- TODO: we should set up an alert for this, but not fail the query.
  END AS enterprise_classification,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.enterprise_and_consumer_metric_segment_clients_v1`
