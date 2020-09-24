CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.account_ecosystem_derived.ecosystem_client_id_lookup_v1`(
    ecosystem_client_id_hash STRING NOT NULL,
    canonical_id STRING NOT NULL,
    first_seen_date DATE NOT NULL
  )
PARTITION BY
  first_seen_date
