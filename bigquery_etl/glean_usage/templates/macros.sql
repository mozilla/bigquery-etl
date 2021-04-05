{% macro core_clients_first_seen(migration_table) %}
-- this lookup is ~13GB on release (org_mozilla_firefox) as of 2021-03-31
_fennec_id_lookup AS (
  SELECT
    client_info.client_id,
    MIN(metrics.uuid.migration_telemetry_identifiers_fennec_client_id) AS fennec_client_id
  FROM
    `{{ migration_table }}`
  WHERE
    DATE(submission_timestamp) > "2010-01-01"
    AND client_info.client_id IS NOT NULL
    AND metrics.uuid.migration_telemetry_identifiers_fennec_client_id IS NOT NULL
  GROUP BY 1
),
_core AS (
  SELECT *
  FROM `telemetry_derived.core_clients_first_seen_v1`
  WHERE first_seen_date > "2010-01-01"
),
-- scanning this table is ~25GB
_core_clients_first_seen AS (
  SELECT
    _fennec_id_lookup.client_id,
    first_seen_date
  FROM
    _fennec_id_lookup
  JOIN
     _core
  ON _fennec_id_lookup.fennec_client_id = _core.client_id
)
{% endmacro %}
