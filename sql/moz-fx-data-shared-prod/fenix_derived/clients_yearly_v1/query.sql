{% if is_init() %}
  SELECT
    CAST(NULL AS BYTES) AS days_seen_bytes,
    * EXCEPT (normalized_app_id),
  FROM
    `moz-fx-data-shared-prod.fenix.baseline_clients_daily`
  WHERE
  -- Output empty table and read no input rows
    FALSE
{% else %}
  WITH base AS (
  -- There are duplicates now in `baseline_clients_daily` because of the join with `clients_first_seen`,
  -- so we take the minimum
    SELECT
      * EXCEPT (normalized_app_id),
      ROW_NUMBER() OVER (
        PARTITION BY
          client_id,
          submission_date
        ORDER BY
          first_seen_date ASC,
          first_run_date ASC
      ) AS rn,
    FROM
      `moz-fx-data-shared-prod.fenix.baseline_clients_daily`
    WHERE
      submission_date = @submission_date
  ),
  _current AS (
    SELECT
      -- In this raw table, we capture the history of activity over the past
      -- 365 days for each usage criterion as an array of bytes. The
      -- rightmost bit represents whether the user was active in the current day.
      `moz-fx-data-shared-prod.udf.bool_to_365_bits`(TRUE) AS days_seen_bytes,
      -- We explicitly pull out the fields specified in schema.yaml
      client_id,
      sample_id,
      first_run_date,
      durations,
      days_seen_session_start_bits,
      days_seen_session_end_bits,
      normalized_channel,
      normalized_os,
      normalized_os_version,
      android_sdk_version,
      locale,
      city,
      country,
      app_build,
      app_channel,
      app_display_version,
      architecture,
      device_manufacturer,
      device_model,
      telemetry_sdk_build,
      first_seen_date,
      is_new_profile,
      isp,
      distribution_id,
      geo_subdivision,
    FROM
      base
    WHERE
      rn = 1
  ),
  _previous AS (
    SELECT
      * EXCEPT (submission_date)
    FROM
      `moz-fx-data-shared-prod.fenix_derived.clients_yearly_v1`
    WHERE
      submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
      -- Filter out rows from yesterday that have now fallen outside the 365-day window.
      AND BIT_COUNT(`moz-fx-data-shared-prod.udf.shift_365_bits_one_day`(days_seen_bytes)) > 0
  )
  SELECT
    @submission_date AS submission_date,
    IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
      `moz-fx-data-shared-prod.udf.combine_adjacent_days_365_bits`(
        _previous.days_seen_bytes,
        _current.days_seen_bytes
      ) AS days_seen_bytes
    )
  FROM
    _current
  FULL OUTER JOIN
    _previous
  -- Include sample_id to match the clustering of the tables, which may improve
  -- join performance.
    USING (sample_id, client_id)
{% endif %}
