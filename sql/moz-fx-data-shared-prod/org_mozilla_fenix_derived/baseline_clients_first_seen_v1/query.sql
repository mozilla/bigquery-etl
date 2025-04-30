-- Generated via bigquery_etl.glean_usage
{% if is_init() %}
  WITH baseline AS (
    SELECT
      client_info.client_id,
      -- Some Glean data from 2019 contains incorrect sample_id, so we
      -- recalculate here; see bug 1707640
      `moz-fx-data-shared-prod.udf.safe_sample_id`(client_info.client_id) AS sample_id,
      DATE(MIN(submission_timestamp)) AS submission_date,
      DATE(MIN(submission_timestamp)) AS first_seen_date,
      ARRAY_AGG(client_info.attribution ORDER BY submission_timestamp DESC LIMIT 1)[
        OFFSET(0)
      ] AS attribution,
      ARRAY_AGG(client_info.distribution ORDER BY submission_timestamp DESC LIMIT 1)[
        OFFSET(0)
      ] AS `distribution`
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_stable.baseline_v1`
    -- initialize by looking over all of history
    WHERE
      DATE(submission_timestamp) > "2010-01-01"
    GROUP BY
      client_id,
      sample_id
  ),
-- this lookup is ~13GB on release (org_mozilla_firefox) as of 2021-03-31
  _fennec_id_lookup AS (
    SELECT DISTINCT
      client_info.client_id,
      metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_client_id
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_stable.migration_v1`
    WHERE
      DATE(submission_timestamp) > "2010-01-01"
      AND client_info.client_id IS NOT NULL
      AND metrics.uuid.migration_telemetry_identifiers_fennec_client_id IS NOT NULL
  ),
  _core AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.core_clients_first_seen_v1`
    WHERE
      first_seen_date > "2010-01-01"
  ),
-- scanning this table is ~25GB
  _core_clients_first_seen AS (
    SELECT
      _fennec_id_lookup.client_id,
      MIN(first_seen_date) AS first_seen_date,
    FROM
      _fennec_id_lookup
    JOIN
      _core
      ON _fennec_id_lookup.fennec_client_id = _core.client_id
    GROUP BY
      _fennec_id_lookup.client_id
  )
  SELECT
    client_id,
    submission_date,
    COALESCE(core.first_seen_date, baseline.first_seen_date) AS first_seen_date,
    sample_id,
    attribution,
    `distribution`
  FROM
    baseline
  LEFT JOIN
    _core_clients_first_seen AS core
    USING (client_id)
{% else %}
  WITH
-- this lookup is ~13GB on release (org_mozilla_firefox) as of 2021-03-31
  _fennec_id_lookup AS (
    SELECT DISTINCT
      client_info.client_id,
      metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_client_id
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_stable.migration_v1`
    WHERE
      DATE(submission_timestamp) > "2010-01-01"
      AND client_info.client_id IS NOT NULL
      AND metrics.uuid.migration_telemetry_identifiers_fennec_client_id IS NOT NULL
  ),
  _core AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.core_clients_first_seen_v1`
    WHERE
      first_seen_date > "2010-01-01"
  ),
-- scanning this table is ~25GB
  _core_clients_first_seen AS (
    SELECT
      _fennec_id_lookup.client_id,
      MIN(first_seen_date) AS first_seen_date,
    FROM
      _fennec_id_lookup
    JOIN
      _core
      ON _fennec_id_lookup.fennec_client_id = _core.client_id
    GROUP BY
      _fennec_id_lookup.client_id
  ),
  _baseline AS (
  -- extract the client_id into the top level for the `USING` clause
    SELECT
      client_info.client_id,
    -- Some Glean data from 2019 contains incorrect sample_id, so we
    -- recalculate here; see bug 1707640
      `moz-fx-data-shared-prod.udf.safe_sample_id`(client_info.client_id) AS sample_id,
      ARRAY_AGG(client_info.attribution ORDER BY submission_timestamp DESC LIMIT 1)[
        OFFSET(0)
      ] AS attribution,
      ARRAY_AGG(client_info.distribution ORDER BY submission_timestamp DESC LIMIT 1)[
        OFFSET(0)
      ] AS `distribution`
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_stable.baseline_v1`
    WHERE
      DATE(submission_timestamp) = @submission_date
      AND client_info.client_id IS NOT NULL -- Bug 1896455
    GROUP BY
      client_id,
      sample_id
  ),
  _current AS (
    SELECT
      @submission_date AS submission_date,
      COALESCE(first_seen_date, @submission_date) AS first_seen_date,
      sample_id,
      client_id,
      attribution,
      `distribution`
    FROM
      _baseline
    LEFT JOIN
      _core_clients_first_seen
      USING (client_id)
  ),
  _previous AS (
    SELECT
      fs.submission_date,
      IF(
        core IS NOT NULL
        AND core.first_seen_date <= fs.first_seen_date,
        core.first_seen_date,
        fs.first_seen_date
      ) AS first_seen_date,
      sample_id,
      client_id,
      attribution,
      `distribution`
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_derived.baseline_clients_first_seen_v1` fs
    LEFT JOIN
      _core_clients_first_seen core
      USING (client_id)
    WHERE
      fs.first_seen_date > "2010-01-01"
      AND fs.first_seen_date < @submission_date
  ),
  _joined AS (
    SELECT
      IF(
        _previous.client_id IS NULL
        OR _previous.first_seen_date >= _current.first_seen_date,
        _current,
        _previous
      ).*
    FROM
      _current
    FULL JOIN
      _previous
      USING (client_id)
  )
-- added this as the result of bug#1788650
  SELECT
    submission_date,
    first_seen_date,
    sample_id,
    client_id,
    attribution,
    `distribution`
  FROM
    _joined
  QUALIFY
    IF(COUNT(*) OVER (PARTITION BY client_id) > 1, ERROR("duplicate client_id detected"), TRUE)
{% endif %}
