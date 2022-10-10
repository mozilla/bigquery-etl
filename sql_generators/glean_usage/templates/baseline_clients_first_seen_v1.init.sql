{{ header }}
{% from "macros.sql" import core_clients_first_seen %}

CREATE TABLE IF NOT EXISTS
  `{{ first_seen_table }}`
PARTITION BY
  first_seen_date
CLUSTER BY
  sample_id,
  submission_date
OPTIONS
  (require_partition_filter = FALSE)
AS
WITH
  baseline AS (
    SELECT
      client_info.client_id,
      -- Some Glean data from 2019 contains incorrect sample_id, so we
      -- recalculate here; see bug 1707640
      udf.safe_sample_id(client_info.client_id) AS sample_id,
      DATE(MIN(submission_timestamp)) as submission_date,
      DATE(MIN(submission_timestamp)) as first_seen_date,
    FROM
      `{{ baseline_table }}`
    -- initialize by looking over all of history
    WHERE
      DATE(submission_timestamp) > "2010-01-01"
    GROUP BY
      client_id,
      sample_id
  )
{% if fennec_id %}
  ,
  {{ core_clients_first_seen(migration_table) }}
  SELECT
    client_id,
    submission_date,
    COALESCE(core.first_seen_date, baseline.first_seen_date) AS first_seen_date,
    sample_id,
  FROM baseline
  LEFT JOIN _core_clients_first_seen AS core
  USING (client_id)
{% else %}
  SELECT * FROM baseline
{% endif %}
