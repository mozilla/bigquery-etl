{% if is_init() %}
/*
We cannot create the entire table in a single query, as it exceeds the
shuffle size limit, presumably because it has to distribute the entire
clients_first_seen_dates table to all nodes to join against.

By having the @sample_id parameter in the query, ./bqetl query initialize will
run the query for each sample ID separately. Reduce parallelism to ensure on-demand
query slots aren't exceeded.
*/
  WITH base AS (
    SELECT
      client_id,
      ARRAY_AGG(submission_date ORDER BY submission_date) AS dates_seen,
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
    WHERE
      submission_date >= DATE('2010-01-01')
      AND sample_id = @sample_id
    GROUP BY
      client_id
  ),
  clients_first_seen_dates AS (
    SELECT
      client_id,
      IF(ARRAY_LENGTH(dates_seen) > 0, dates_seen[OFFSET(0)], NULL) AS first_seen_date,
      IF(ARRAY_LENGTH(dates_seen) > 1, dates_seen[OFFSET(1)], NULL) AS second_seen_date,
    FROM
      base
  )
  SELECT
    cfsd.first_seen_date,
    cfsd.second_seen_date,
    cd.* EXCEPT (submission_date)
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6` AS cd
  LEFT JOIN
    clients_first_seen_dates AS cfsd
    ON (cd.submission_date = cfsd.first_seen_date AND cd.client_id = cfsd.client_id)
  WHERE
    cfsd.client_id IS NOT NULL
    AND cd.submission_date >= DATE('2010-01-01')
    AND sample_id = @sample_id;

{% else %}
  WITH today AS (
    SELECT
      CAST(NULL AS DATE) AS first_seen_date,
      CAST(NULL AS DATE) AS second_seen_date,
      * EXCEPT (submission_date)
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
    WHERE
      submission_date = @submission_date
  ),
  previous AS (
  -- If we need to reprocess data, we have to make sure to delete all data
  -- earlier than the passed @submission_date parameter, so we null out
  -- invalid second_seen_date values and drop rows invalid first_seen_date.
    SELECT
      * REPLACE (
        IF(second_seen_date >= @submission_date, NULL, second_seen_date) AS second_seen_date
      )
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v1`
    WHERE
      first_seen_date < @submission_date
  )
  SELECT
  -- Only insert dimensions from clients_daily if this is the first time the
  -- client has been seen; otherwise, we copy over the existing dimensions
  -- from the first sighting.
    IF(previous.client_id IS NULL, today, previous).* REPLACE (
      IF(
        previous.first_seen_date IS NULL,
        @submission_date,
        previous.first_seen_date
      ) AS first_seen_date,
      IF(
        previous.second_seen_date IS NULL
        AND previous.first_seen_date IS NOT NULL
        AND today.client_id IS NOT NULL,
        @submission_date,
        previous.second_seen_date
      ) AS second_seen_date
    )
  FROM
    previous
  FULL JOIN
    today
    USING (client_id)
{% endif %}
