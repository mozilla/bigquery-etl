{% set usage_types = [("seen", "TRUE"), ("active", "durations > 0")] %}
{% if is_init() %}
  CREATE TABLE IF NOT EXISTS
    `moz-fx-data-shared-prod`.firefox_ios_derived.baseline_clients_yearly_v1
  PARTITION BY
    submission_date
  CLUSTER BY
    normalized_channel,
    sample_id
  OPTIONS
    (require_partition_filter = TRUE)
  AS
  SELECT
    {% for usage_type, _ in usage_types %}
      CAST(NULL AS BYTES) AS `days_{{ usage_type }}_bytes`,
    {% endfor %}
    -- We make sure to delay * until the end so that as new columns are added
    -- to the daily table we can add those columns in the same order to the end
    -- of this schema, which may be necessary for the daily join query between
    -- the two tables to validate.
    *
  FROM
    `moz-fx-data-shared-prod`.firefox_ios.baseline_clients_daily
  WHERE
    -- Output empty table and read no input rows
    FALSE
{% else %}
  WITH _current AS (
    SELECT
      -- In this raw table, we capture the history of activity over the past
      -- 365 days for each usage criterion as an array of bytes. The
      -- rightmost bit represents whether the user was active in the current day.
      {% for usage_type, criterion in usage_types %}
        `moz-fx-data-shared-prod.udf.bool_to_365_bits`(
          {{ criterion }}
        ) AS `days_{{ usage_type }}_bytes`,
      {% endfor %}
      -- List cols explicitly here: the schema is static (schema.yaml),
      -- and new columns added upstream will need to be manually added
      normalized_app_id,
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
      `moz-fx-data-shared-prod`.firefox_ios.baseline_clients_daily
    WHERE
      submission_date = @submission_date
      AND sample_id IS NOT NULL
  ),
  --
  _previous AS (
    SELECT
      * EXCEPT (submission_date)
    FROM
      `moz-fx-data-shared-prod`.firefox_ios_derived.baseline_clients_yearly_v1
    WHERE
      submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
      -- Filter out rows from yesterday that have now fallen outside the 365-day window.
      AND BIT_COUNT(`moz-fx-data-shared-prod.udf.shift_365_bits_one_day`(days_seen_bytes)) > 0
      AND sample_id IS NOT NULL
  )
  --
  SELECT
    @submission_date AS submission_date,
    IF(_current.client_id IS NOT NULL, _current, _previous).* REPLACE (
      {% for usage_type, _ in usage_types %}
        `moz-fx-data-shared-prod.udf.combine_adjacent_days_365_bits`(
          _previous.`days_{{ usage_type }}_bytes`,
          _current.`days_{{ usage_type }}_bytes`
        ) AS `days_{{ usage_type }}_bytes` {{ "," if not loop.last }}
      {% endfor %}
    )
  FROM
    _current
  FULL JOIN
    _previous
    USING (client_id)
{% endif %}
