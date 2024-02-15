-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.moso_mastodon_android.baseline_clients_last_seen`
AS
SELECT
  "org_mozilla_social_nightly" AS normalized_app_id,
  * REPLACE ("nightly" AS normalized_channel)
FROM
  `moz-fx-data-shared-prod.org_mozilla_social_nightly.baseline_clients_last_seen`
