{{ header }}

{% if init %}

CREATE TABLE IF NOT EXISTS
  `{{ first_seen_table }}`
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  DATE(submission_timestamp) as submission_date,
  sample_id,
  client_info.client_id
FROM
  `{{ baseline_table }}`
WHERE
  FALSE

{% else %}

WITH
{% if fennec_id %}
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
-- scanning this table is ~25GB
_core_clients_first_seen AS (
  SELECT
    lookup.client_id,
    first_seen_date
  FROM
    _fennec_id_lookup lookup
  JOIN
    `moz-fx-data-shared-prod.telemetry_derived.core_clients_first_seen_v1` core
  ON lookup.fennec_client_id = core.client_id

),
_current AS (
  SELECT
    COALESCE(first_seen_date, @submission_date) as submission_date,
    sample_id,
    client_info.client_id
  FROM
    `{{ baseline_table }}`
  LEFT JOIN
    _core_clients_first_seen
  USING
    (client_id)
  WHERE
    DATE(submission_timestamp) = @submission_date
),
{% else %}
_current AS (
  SELECT DISTINCT
    @submission_date as submission_date,
    sample_id,
    client_info.client_id
  FROM
    `{{ baseline_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
    and client_info.client_id IS NOT NULL
),
{% endif %}
  -- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    *
  FROM
    `{{ first_seen_table }}`
  WHERE
    submission_date < @submission_date
)
  --
SELECT
  _current.*
FROM
  _current
LEFT JOIN
  _previous
USING
  (client_id)
WHERE _previous.client_id IS NULL

{% endif %}
