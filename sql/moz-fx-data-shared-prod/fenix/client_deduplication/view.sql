CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.client_deduplication`
AS
WITH unioned AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.client_deduplication`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.client_deduplication`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.client_deduplication`
),
with_ad_id AS (
  SELECT
    client_info.client_id,
    COALESCE(
      metrics.string.client_deduplication_hashed_gaid,
      metrics.string.activation_identifier
    ) AS hashed_ad_id,
    *
  FROM
    unioned
)
-- pseudonymize_ad_ids sets opted-out Ad IDs to NULL,
-- but because they come in as all-0s, the client thought
-- they were valid. Here, we check for the NULLed ad_id
-- and indicate the Ad ID is not valid in valid_advertising_id
SELECT
  client_id,
  hashed_ad_id,
  metrics.boolean.client_deduplication_valid_advertising_id
  AND hashed_ad_id IS NOT NULL AS valid_advertising_id,
  * EXCEPT (client_id, hashed_ad_id)
FROM
  with_ad_id
